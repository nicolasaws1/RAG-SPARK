# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Ingestão e preparação dos PDFs (Semana 2)
# MAGIC
# MAGIC **Entrada:** os PDFs de `data/pdfs/` (copiados para um Volume do Unity Catalog).
# MAGIC **Saída:** duas tabelas estruturadas:
# MAGIC
# MAGIC | Tabela | Grão | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | `documentos` | 1 linha por PDF | tipo de norma, número, ano, órgão, processo SEI, exercício do VAAR, condicionalidades, assunto |
# MAGIC | `paginas` | 1 linha por página | texto bruto, nº de tabelas detectadas, tabelas em Markdown |
# MAGIC
# MAGIC **Particionamento:** cada PDF é dividido em faixas de `PAGINAS_POR_TAREFA` páginas e cada faixa
# MAGIC vira uma tarefa. Sem isso, a Portaria (169 páginas) ficaria numa única tarefa e atrasaria o job
# MAGIC inteiro (*data skew*), enquanto os outros 8 PDFs têm de 1 a 8 páginas.

# COMMAND ----------

# MAGIC %pip install pymupdf --quiet

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./funcoes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparar o Volume (só no Databricks)
# MAGIC Cria o schema e o volume, e copia os PDFs versionados no repositório para dentro dele.

# COMMAND ----------

import shutil

if IS_DATABRICKS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME}")
    origem = os.path.join(REPO, "data", "pdfs")
    for nome in sorted(os.listdir(origem)):
        if nome.lower().endswith(".pdf"):
            shutil.copy(os.path.join(origem, nome), os.path.join(PDF_DIR, nome))
    print(f"{len(os.listdir(PDF_DIR))} arquivos em {PDF_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Leitura binária distribuída
# MAGIC O leitor `binaryFile` do Spark traz cada PDF como uma linha com `path`, `length` e `content` (bytes).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

def ler_binarios_sem_hadoop(pasta):
    """Mesmo schema do binaryFile, lendo os arquivos no driver.

    Só para rodar local no Windows: lá o Spark não lista diretórios sem winutils/hadoop.dll.
    """
    from datetime import datetime

    schema = T.StructType([
        T.StructField("path", T.StringType()),
        T.StructField("modificationTime", T.TimestampType()),
        T.StructField("length", T.LongType()),
        T.StructField("content", T.BinaryType()),
    ])
    linhas = []
    for nome in sorted(os.listdir(pasta)):
        if nome.lower().endswith(".pdf"):
            caminho = os.path.join(pasta, nome)
            with open(caminho, "rb") as arquivo:
                conteudo = arquivo.read()
            linhas.append(("file:/" + caminho.replace("\\", "/"), datetime.fromtimestamp(os.path.getmtime(caminho)), len(conteudo), bytearray(conteudo)))
    return spark.createDataFrame(linhas, schema)


with medir("leitura_binaria") as m:
    if IS_DATABRICKS or os.name != "nt":
        binarios = spark.read.format("binaryFile").option("pathGlobFilter", "*.pdf").load(PDF_DIR)
    else:
        binarios = ler_binarios_sem_hadoop(PDF_DIR)
    binarios = binarios.withColumn("arquivo", F.element_at(F.split("path", "/"), -1))
    m["arquivos"] = binarios.count()
    m["bytes"] = binarios.agg(F.sum("length")).first()[0]

display(binarios.select("arquivo", "length", "modificationTime"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inventário, deduplicação e metadados
# MAGIC Uma UDF abre cada PDF só para contar páginas, calcular o SHA-256 e ler a primeira página.
# MAGIC O hash elimina cópias do mesmo arquivo com nomes diferentes (a pasta original do grupo tinha
# MAGIC 12 arquivos e só 9 eram distintos). Os metadados saem de expressões regulares sobre a primeira página.

# COMMAND ----------

schema_info = T.StructType([
    T.StructField("n_paginas", T.IntegerType()),
    T.StructField("sha256", T.StringType()),
    T.StructField("primeira_pagina", T.StringType()),
])

schema_meta = T.StructType([
    T.StructField("arquivo", T.StringType()),
    T.StructField("tipo", T.StringType()),
    T.StructField("numero", T.IntegerType()),
    T.StructField("ano", T.IntegerType()),
    T.StructField("orgao", T.StringType()),
    T.StructField("processo_sei", T.StringType()),
    T.StructField("data_publicacao", T.StringType()),
    T.StructField("exercicio_vaar", T.IntegerType()),
    T.StructField("condicionalidades", T.ArrayType(T.StringType())),
    T.StructField("assunto", T.StringType()),
])

info_udf = F.udf(info_pdf, schema_info)
meta_udf = F.udf(extrair_metadados, schema_meta)

# Salvo como tabela (e não com .cache(), que o serverless não aceita): assim as UDFs rodam uma
# vez só e o dropDuplicates não escolhe um arquivo diferente a cada ação.
with medir("inventario_metadados") as m:
    inventario = salvar(
        binarios
        .withColumn("info", info_udf("content"))
        .select("arquivo", "path", "length", "content", "info.*")
        .dropDuplicates(["sha256"])                      # shuffle 1: deduplicação por hash
        .withColumn("doc_id", F.substring("sha256", 1, 12))
        .withColumn("meta", meta_udf("arquivo", "primeira_pagina")),
        "inventario",
    )
    m["documentos_unicos"] = inventario.count()

documentos = salvar(
    inventario.select(
        "doc_id", "arquivo", "n_paginas", F.col("length").alias("bytes"), "sha256",
        "meta.tipo", "meta.numero", "meta.ano", "meta.orgao", "meta.processo_sei",
        "meta.data_publicacao", "meta.exercicio_vaar", "meta.condicionalidades", "meta.assunto",
    ),
    "documentos",
)
display(documentos.drop("sha256", "assunto").orderBy("tipo", "ano", "numero"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Faixas de páginas → tarefas
# MAGIC `faixas_de_paginas` gera as faixas; `explode` vira uma linha por faixa e o `repartitionByRange`
# MAGIC espalha as faixas pelas tarefas (shuffle 2). É aqui que o PDF grande deixa de ser gargalo.

# COMMAND ----------

schema_faixa = T.ArrayType(T.StructType([
    T.StructField("pag_ini", T.IntegerType()),
    T.StructField("pag_fim", T.IntegerType()),
]))
faixas_udf = F.udf(lambda n: faixas_de_paginas(n, PAGINAS_POR_TAREFA), schema_faixa)

faixas = (
    inventario
    .select("doc_id", "content", F.explode(faixas_udf("n_paginas")).alias("faixa"))
    .select("doc_id", "content", "faixa.pag_ini", "faixa.pag_fim")
)
n_faixas = faixas.count()
# Range partitioning: uma faixa por partição (hash poderia juntar duas faixas na mesma tarefa)
faixas = faixas.repartitionByRange(n_faixas, "doc_id", "pag_ini")

display(
    faixas.withColumn("particao", F.spark_partition_id())
    .groupBy("particao").agg(F.count("*").alias("faixas"), F.sum(F.col("pag_fim") - F.col("pag_ini") + 1).alias("paginas"))
    .orderBy("particao")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Extração de texto e tabelas por página
# MAGIC Cada tarefa abre o PDF, lê só a sua faixa, extrai o texto e procura tabelas com `find_tables()`
# MAGIC do PyMuPDF (as tabelas são guardadas em Markdown para o RAG citar depois).

# COMMAND ----------

schema_paginas = T.ArrayType(T.StructType([
    T.StructField("pagina", T.IntegerType()),
    T.StructField("texto", T.StringType()),
    T.StructField("n_tabelas", T.IntegerType()),
    T.StructField("tabelas", T.ArrayType(T.StringType())),
]))
paginas_udf = F.udf(lambda c, a, b: extrair_paginas(c, a, b, DETECTAR_TABELAS), schema_paginas)

with medir("extracao_paginas", faixas=n_faixas, paginas_por_tarefa=PAGINAS_POR_TAREFA) as m:
    paginas = salvar(
        faixas
        .select("doc_id", F.explode(paginas_udf("content", "pag_ini", "pag_fim")).alias("p"))
        .select(
            "doc_id", "p.pagina",
            F.col("p.texto").alias("texto_bruto"),
            F.length("p.texto").alias("n_chars"),
            "p.n_tabelas", "p.tabelas",
        ),
        "paginas",
    )
    m["paginas"] = paginas.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conferência do dataset estruturado

# COMMAND ----------

resumo = (
    paginas.groupBy("doc_id")
    .agg(
        F.count("*").alias("paginas_extraidas"),
        F.sum("n_chars").alias("caracteres"),
        F.sum("n_tabelas").alias("tabelas"),
        F.sum(F.when(F.col("n_chars") < 50, 1).otherwise(0)).alias("paginas_quase_vazias"),
    )
    .join(documentos.select("doc_id", "arquivo", "tipo", "n_paginas"), "doc_id")
    .withColumn("ok", F.col("paginas_extraidas") == F.col("n_paginas"))
    .orderBy(F.desc("caracteres"))
)
display(resumo)
assert resumo.filter(~F.col("ok")).count() == 0, "Alguma página não foi extraída"

totais = resumo.agg(F.sum("paginas_extraidas").alias("paginas"), F.sum("caracteres").alias("caracteres"), F.sum("tabelas").alias("tabelas")).first()
print(f"📄 Ingestão concluída | Documentos: {resumo.count()} | Páginas: {totais['paginas']} | "
      f"Caracteres: {totais['caracteres']:,} | Tabelas detectadas: {totais['tabelas']}")
for linha in documentos.orderBy("tipo", "ano", "numero").collect():
    print(f"   {linha['tipo']:<26} nº {linha['numero']}/{linha['ano']} | {linha['orgao'] or '-':<7} | exercício {linha['exercicio_vaar']} | "
          f"condicionalidades {', '.join(linha['condicionalidades'] or []) or '-'} | {linha['n_paginas']} pág.")

# COMMAND ----------

import json

salvar(spark.createDataFrame([
    {"etapa": r["etapa"], "segundos": float(r["segundos"]),
     "detalhes": json.dumps({k: v for k, v in r.items() if k not in ("etapa", "segundos")}, ensure_ascii=False)}
    for r in METRICAS
]), "metricas_ingestao")
