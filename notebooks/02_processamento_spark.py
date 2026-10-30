# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 · Processamento distribuído com Spark (Semana 3)
# MAGIC
# MAGIC Parte das tabelas `paginas` e `documentos` (notebook 01) e:
# MAGIC
# MAGIC 1. **Limpa** o texto só com funções nativas do Spark (rodam na JVM, sem UDF Python);
# MAGIC 2. **Reagrupa** as páginas em um texto por documento (`groupBy` → shuffle);
# MAGIC 3. **Compara** dois planos de *join* com a tabela de metadados: *broadcast* (sem shuffle) e *sort-merge* (com shuffle);
# MAGIC 4. **Mede** tempo e volume de cada etapa e salva tudo na tabela `metricas`.
# MAGIC
# MAGIC Evidência do shuffle: os planos físicos impressos abaixo (operador `Exchange`) e, no Databricks,
# MAGIC o *Query profile* de cada célula (ícone "See performance" abaixo da saída).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

paginas = ler("paginas")
documentos = ler("documentos")
print(f"{documentos.count()} documentos | {paginas.count()} páginas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Limpeza nativa
# MAGIC Diferente da versão anterior do notebook, **não** converte para minúsculas e **não** apaga `º`, `ª` e `§`
# MAGIC (eles fazem parte de referências como "art. 14, § 1º"). O que sai:
# MAGIC carimbo de assinatura digital do DOU, ligaturas tipográficas (`ﬁ` → `fi`), hifenização de fim de linha
# MAGIC e espaços repetidos.

# COMMAND ----------

CARIMBO_DOU = r"Documento assinado digitalmente conforme MP n[ºo°] ?2\.200-2.*?pelo c[óo]digo \d+"


def limpar(coluna):
    texto = F.col(coluna)
    texto = F.regexp_replace(texto, "ﬁ", "fi")
    texto = F.regexp_replace(texto, "ﬂ", "fl")
    texto = F.regexp_replace(texto, r"(?s)" + CARIMBO_DOU, " ")
    texto = F.regexp_replace(texto, r"(\p{L})-\n(\p{Ll})", "$1$2")    # hifenização no fim da linha
    texto = F.regexp_replace(texto, r"[ \t ]+", " ")
    texto = F.regexp_replace(texto, r"\s*\n\s*", "\n")
    return F.trim(texto)


with medir("limpeza") as m:
    paginas_limpas = (
        paginas
        .withColumn("texto", limpar("texto_bruto"))
        .withColumn("n_chars_limpo", F.length("texto"))
        .withColumn("n_palavras", F.size(F.split("texto", r"\s+")))
        .filter(F.col("n_chars_limpo") > 0)
    )
    paginas_limpas = salvar(paginas_limpas.drop("texto_bruto"), "paginas_limpas")
    m["paginas"] = paginas_limpas.count()
    m["chars_antes"] = paginas.agg(F.sum("n_chars")).first()[0]
    m["chars_depois"] = paginas_limpas.agg(F.sum("n_chars_limpo")).first()[0]

reducao = 100 * (1 - m["chars_depois"] / m["chars_antes"])
print(f"🧹 Limpeza concluída | Páginas: {m['paginas']} | Caracteres: {m['chars_antes']:,} → {m['chars_depois']:,} (-{reducao:.1f}%)")

display(paginas_limpas.select("doc_id", "pagina", F.substring("texto", 1, 200).alias("inicio")).orderBy("doc_id", "pagina"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Texto por documento (`groupBy` + shuffle)
# MAGIC As páginas de um mesmo PDF estão espalhadas em partições diferentes (vieram de faixas distintas).
# MAGIC Para montar o texto contínuo, o Spark precisa trazer todas para a mesma partição: isso é o shuffle.
# MAGIC Cada página recebe um marcador `[p. N]` para o RAG conseguir citar a página depois.

# COMMAND ----------

texto_por_doc = (
    paginas_limpas
    .groupBy("doc_id")
    .agg(
        F.array_sort(F.collect_list(F.struct("pagina", "texto"))).alias("paginas"),
        F.sum("n_palavras").alias("n_palavras"),
        F.sum("n_tabelas").alias("n_tabelas"),
    )
    .withColumn(
        "texto",
        F.concat_ws("\n\n", F.transform("paginas", lambda p: F.concat(F.lit("[p. "), p["pagina"].cast("string"), F.lit("] "), p["texto"]))),
    )
    .drop("paginas")
)

plano_groupby = plano(texto_por_doc)
print(plano_groupby)

with medir("groupby_texto_por_doc") as m:
    texto_por_doc = salvar(texto_por_doc, "texto_por_doc")
    m["documentos"] = texto_por_doc.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Join com metadados: broadcast × sort-merge
# MAGIC A tabela `documentos` é minúscula, então o Spark prefere **broadcast**: copia ela inteira para cada
# MAGIC executor (`BroadcastExchange`, que não é shuffle). Forçando `merge` (sort-merge join), os dois lados
# MAGIC precisam estar particionados por `doc_id`. O lado das estatísticas já está, por causa do `groupBy`;
# MAGIC o lado `documentos` ganha um `Exchange hashpartitioning` a mais, que é o shuffle extra do join.

# COMMAND ----------

estatisticas = paginas_limpas.groupBy("doc_id").agg(
    F.count("*").alias("paginas"),
    F.sum("n_palavras").alias("palavras"),
    F.sum("n_tabelas").alias("tabelas"),
    F.round(F.avg("n_palavras"), 1).alias("palavras_por_pagina"),
)
meta = documentos.select("doc_id", "arquivo", "tipo", "orgao", "ano", "exercicio_vaar", "condicionalidades")

join_broadcast = estatisticas.join(meta.hint("broadcast"), "doc_id")
join_merge = estatisticas.join(meta.hint("merge"), "doc_id")

def n_shuffles(texto_plano: str) -> int:
    # Cada shuffle aparece como um Exchange com "hashpartitioning(...)" nos argumentos
    return texto_plano.count("hashpartitioning(")


planos_join = {"broadcast": plano(join_broadcast), "merge": plano(join_merge)}
for nome, texto_plano in planos_join.items():
    print(f"===== {nome}: {n_shuffles(texto_plano)} shuffle(s) =====")
    print(texto_plano)

for nome, df in [("broadcast", join_broadcast), ("merge", join_merge)]:
    with medir(f"join_{nome}", shuffles=n_shuffles(planos_join[nome])) as m:
        m["linhas"] = df.count()

display(join_broadcast.orderBy(F.desc("palavras")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agregação por tipo de norma e por condicionalidade

# COMMAND ----------

por_tipo = join_broadcast.groupBy("tipo").agg(
    F.count("*").alias("documentos"),
    F.sum("paginas").alias("paginas"),
    F.sum("palavras").alias("palavras"),
    F.sum("tabelas").alias("tabelas"),
).orderBy(F.desc("palavras"))
display(por_tipo)

por_condicionalidade = (
    meta.select("arquivo", "exercicio_vaar", F.explode("condicionalidades").alias("condicionalidade"))
    .groupBy("condicionalidade")
    .agg(F.count("*").alias("documentos"), F.array_sort(F.collect_set("exercicio_vaar")).alias("exercicios"))
    .orderBy("condicionalidade")
)
display(por_condicionalidade)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Distribuição das páginas entre partições
# MAGIC Mostra como as linhas ficaram espalhadas antes e depois de um `repartition` por `doc_id`.

# COMMAND ----------

def distribuicao(df):
    return (
        df.withColumn("particao", F.spark_partition_id())
        .groupBy("particao").agg(F.count("*").alias("paginas"), F.countDistinct("doc_id").alias("documentos"))
        .orderBy("particao")
    )


display(distribuicao(paginas_limpas))
display(distribuicao(paginas_limpas.repartition(SHUFFLE_PARTITIONS, "doc_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Métricas

# COMMAND ----------

import json

volume = paginas_limpas.agg(
    F.count("*").alias("paginas"),
    F.sum("n_chars_limpo").alias("caracteres"),
    F.sum("n_palavras").alias("palavras"),
    F.sum("n_tabelas").alias("tabelas"),
).first().asDict()
volume["documentos"] = documentos.count()
volume["bytes_pdf"] = documentos.agg(F.sum("bytes")).first()[0]
print(json.dumps(volume, indent=2, ensure_ascii=False))

metricas = spark.createDataFrame([
    {"etapa": r["etapa"], "segundos": float(r["segundos"]),
     "detalhes": json.dumps({k: v for k, v in r.items() if k not in ("etapa", "segundos")}, ensure_ascii=False)}
    for r in METRICAS
])
salvar(metricas, "metricas")
display(metricas)

if not IS_DATABRICKS:
    destino = os.path.join(REPO, "docs", "resultados")
    os.makedirs(destino, exist_ok=True)
    with open(os.path.join(destino, "metricas_local.json"), "w", encoding="utf-8") as arquivo:
        json.dump({"volume": volume, "etapas": METRICAS}, arquivo, indent=2, ensure_ascii=False)
    with open(os.path.join(destino, "planos_local.txt"), "w", encoding="utf-8") as arquivo:
        arquivo.write("### groupBy texto_por_doc\n" + plano_groupby)
        for nome, texto_plano in planos_join.items():
            arquivo.write(f"\n### join {nome}\n" + texto_plano)
