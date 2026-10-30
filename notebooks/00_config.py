# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuração
# MAGIC
# MAGIC Carregado pelos outros notebooks com `%run ./00_config`. Detecta se está no Databricks
# MAGIC ou rodando local (`scripts/rodar_local.py`) e define caminhos, parâmetros e utilitários.

# COMMAND ----------

import contextlib
import io
import os
import time

IS_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

# ─── Parâmetros do pipeline ──────────────────────────────────
CATALOGO = "workspace"          # catálogo padrão do Databricks Free Edition
SCHEMA = "rag_spark"
VOLUME = "pdfs"
PAGINAS_POR_TAREFA = 20         # cada faixa de 20 páginas vira uma tarefa Spark
SHUFFLE_PARTITIONS = 8          # "shuffle pequeno": poucos dados, poucas partições
DETECTAR_TABELAS = True
CHUNK_SIZE = 800                # caracteres por chunk (o notebook da disciplina usa 300)
CHUNK_OVERLAP = 150             # sobreposição entre chunks consecutivos
CHUNK_MINIMO = 50               # descarta pedaços menores que isso (rodapés, números de página)
# ─────────────────────────────────────────────────────────────

if IS_DATABRICKS:
    # Notebook dentro de um Git folder: o diretório atual é /Workspace/.../RAG-SPARK/notebooks
    REPO = os.path.abspath(os.path.join(os.getcwd(), ".."))
    PDF_DIR = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
else:
    REPO = os.environ.get("RAG_SPARK_REPO", os.path.abspath(os.path.join(os.getcwd(), "..")))
    PDF_DIR = os.path.join(REPO, "data", "pdfs")

try:
    spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
except Exception as erro:  # alguns ambientes serverless bloqueiam configs
    print(f"Aviso: não foi possível ajustar shuffle.partitions ({erro})")

print(f"Ambiente : {'Databricks' if IS_DATABRICKS else 'local'} | Spark {spark.version}")
print(f"PDFs     : {PDF_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistência
# MAGIC No Databricks cada etapa vira uma tabela Delta em `workspace.rag_spark`.
# MAGIC Localmente as tabelas ficam em cache como views temporárias (o Spark no Windows
# MAGIC não grava Parquet sem `winutils`).

# COMMAND ----------


def nome_tabela(nome: str) -> str:
    return f"{CATALOGO}.{SCHEMA}.{nome}" if IS_DATABRICKS else nome


def salvar(df, nome: str):
    if IS_DATABRICKS:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome_tabela(nome))
        return spark.table(nome_tabela(nome))
    df = df.cache()
    df.count()
    df.createOrReplaceTempView(nome)
    return df


def ler(nome: str):
    return spark.table(nome_tabela(nome))


if not IS_DATABRICKS and "display" not in globals():
    def display(df, n: int = 20):
        df.show(n, truncate=80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medição de tempo e plano de execução

# COMMAND ----------

if "METRICAS" not in globals():
    METRICAS = []


@contextlib.contextmanager
def medir(etapa: str, **extra):
    """Mede o tempo de uma etapa. Chame uma ação (count, write) dentro do bloco."""
    inicio = time.perf_counter()
    registro = {"etapa": etapa, **extra}
    yield registro
    registro["segundos"] = round(time.perf_counter() - inicio, 3)
    METRICAS.append(registro)
    print(f"⏱  {etapa}: {registro['segundos']}s {({k: v for k, v in registro.items() if k not in ('etapa', 'segundos')})}")


def plano(df, modo: str = "formatted") -> str:
    """Retorna o plano físico como texto (funciona no Spark clássico e no Spark Connect)."""
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode=modo)
    return buffer.getvalue()
