# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 03 · Chunking
# MAGIC
# MAGIC Corresponde à **Aula 3 (Chunking)** do notebook da disciplina (`rag_e_chat_pipeline_spark.ipynb`).
# MAGIC Parte da tabela `paginas_limpas` (notebook 02) e gera a tabela `chunks`, que será a base dos embeddings.
# MAGIC
# MAGIC | | Notebook da disciplina | Este notebook |
# MAGIC |---|---|---|
# MAGIC | Tamanho / sobreposição | 300 / 50 caracteres | `CHUNK_SIZE` / `CHUNK_OVERLAP` (800 / 150) |
# MAGIC | Onde corta | no caractere exato, no meio da palavra | no último espaço antes do limite |
# MAGIC | Identificador | `monotonically_increasing_id()` (muda a cada execução) | `doc_id-pagina-tipo-indice` (estável) |
# MAGIC | Tabelas do PDF | não trata | cada tabela vira um chunk próprio, em Markdown |
# MAGIC | Metadados | só a página | página + tipo de norma, ano, exercício e condicionalidades (para citar a fonte) |
# MAGIC
# MAGIC 800 caracteres cabem num item numerado inteiro das notas técnicas ("4.2. A Condicionalidade II...");
# MAGIC com 300, a maioria dos itens ficaria partida em três pedaços.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %run ./funcoes

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

paginas_limpas = ler("paginas_limpas")
documentos = ler("documentos")

print(f"📥 Entrada: {documentos.count()} documentos | {paginas_limpas.count()} páginas limpas")
print(f"⚙️  CHUNK_SIZE={CHUNK_SIZE} | CHUNK_OVERLAP={CHUNK_OVERLAP} | CHUNK_MINIMO={CHUNK_MINIMO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Chunks de texto
# MAGIC Uma UDF aplica `dividir_em_chunks` em cada página e o `posexplode` transforma a lista em linhas,
# MAGIC guardando a posição do chunk dentro da página (`chunk_index`). O chunking é por página, então
# MAGIC todo chunk sabe exatamente de qual página veio.

# COMMAND ----------

chunk_udf = F.udf(
    lambda texto: dividir_em_chunks(texto, CHUNK_SIZE, CHUNK_OVERLAP, CHUNK_MINIMO),
    T.ArrayType(T.StringType()),
)

chunks_texto = (
    paginas_limpas
    .select("doc_id", "pagina", F.posexplode(chunk_udf("texto")).alias("chunk_index", "chunk_text"))
    .withColumn("tipo_chunk", F.lit("texto"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Chunks de tabela
# MAGIC As tabelas detectadas na ingestão (PyMuPDF `find_tables`) viram chunks inteiros, sem quebrar,
# MAGIC para o RAG conseguir responder perguntas sobre valores que só existem em tabela.

# COMMAND ----------

chunks_tabela = (
    paginas_limpas
    .filter(F.col("n_tabelas") > 0)
    .select("doc_id", "pagina", F.posexplode("tabelas").alias("chunk_index", "chunk_text"))
    .withColumn("tipo_chunk", F.lit("tabela"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Junta, identifica e anexa os metadados
# MAGIC O `join` com `documentos` usa *broadcast* (a tabela tem uma linha por PDF), então não gera shuffle.

# COMMAND ----------

meta = documentos.select(
    "doc_id", "arquivo",
    F.col("tipo").alias("tipo_norma"), "numero", "ano", "orgao", "exercicio_vaar", "condicionalidades",
)

with medir("chunking") as m:
    chunks = salvar(
        chunks_texto.unionByName(chunks_tabela)
        .withColumn("chunk_id", F.concat_ws("-", "doc_id", F.col("pagina").cast("string"), "tipo_chunk", F.col("chunk_index").cast("string")))
        .withColumn("chunk_len", F.length("chunk_text"))
        .join(meta.hint("broadcast"), "doc_id")
        .select(
            "chunk_id", "doc_id", "arquivo", "pagina", "tipo_chunk", "chunk_index", "chunk_text", "chunk_len",
            "tipo_norma", "numero", "ano", "orgao", "exercicio_vaar", "condicionalidades",
        ),
        "chunks",
    )
    m["chunks"] = chunks.count()

total = m["chunks"]
por_tipo = {r["tipo_chunk"]: r["n"] for r in chunks.groupBy("tipo_chunk").agg(F.count("*").alias("n")).collect()}
stats = chunks.filter(F.col("tipo_chunk") == "texto").agg(
    F.min("chunk_len").alias("min"), F.round(F.avg("chunk_len"), 0).alias("media"), F.max("chunk_len").alias("max")
).first()

print(f"✂️  Chunking concluído | Total de chunks: {total}")
print(f"   Texto : {por_tipo.get('texto', 0)} chunks | tamanho mín {stats['min']} · médio {stats['media']:.0f} · máx {stats['max']} caracteres")
print(f"   Tabela: {por_tipo.get('tabela', 0)} chunks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Chunks por documento

# COMMAND ----------

por_documento = (
    chunks.groupBy("arquivo", "tipo_norma")
    .agg(
        F.countDistinct("pagina").alias("paginas"),
        F.sum(F.when(F.col("tipo_chunk") == "texto", 1).otherwise(0)).alias("chunks_texto"),
        F.sum(F.when(F.col("tipo_chunk") == "tabela", 1).otherwise(0)).alias("chunks_tabela"),
        F.round(F.avg("chunk_len"), 0).alias("tamanho_medio"),
    )
    .orderBy(F.desc("chunks_texto"))
)
display(por_documento)

for linha in por_documento.collect():
    print(f"   {linha['arquivo']:<45} {linha['paginas']:>4} pág. → {linha['chunks_texto']:>5} chunks de texto, {linha['chunks_tabela']:>3} de tabela")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exemplo: dois chunks seguidos e a sobreposição entre eles
# MAGIC O fim do chunk 0 se repete no começo do chunk 1. É isso que evita perder uma frase que cairia
# MAGIC bem na fronteira entre dois pedaços.

# COMMAND ----------

exemplo = (
    chunks.filter((F.col("tipo_chunk") == "texto") & (F.col("chunk_index") <= 1))
    .join(
        chunks.filter((F.col("tipo_chunk") == "texto") & (F.col("chunk_index") == 1)).select("doc_id", "pagina").limit(1),
        ["doc_id", "pagina"],
    )
    .orderBy("chunk_index")
    .collect()
)
for linha in exemplo:
    print(f"── {linha['chunk_id']} ({linha['arquivo']}, p. {linha['pagina']}, {linha['chunk_len']} caracteres)")
    print(linha["chunk_text"])
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Amostra da tabela `chunks`

# COMMAND ----------

display(chunks.select("chunk_id", "arquivo", "pagina", "tipo_chunk", "chunk_len", "tipo_norma", "exercicio_vaar", "condicionalidades", "chunk_text").orderBy("arquivo", "pagina", "tipo_chunk", "chunk_index"))

# COMMAND ----------

import json

salvar(spark.createDataFrame([
    {"etapa": r["etapa"], "segundos": float(r["segundos"]),
     "detalhes": json.dumps({k: v for k, v in r.items() if k not in ("etapa", "segundos")}, ensure_ascii=False)}
    for r in METRICAS
]), "metricas_chunking")
