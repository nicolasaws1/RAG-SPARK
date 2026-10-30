"""Roda os notebooks localmente, em sequência, numa SparkSession local.

Os notebooks estão no formato "Databricks source" (.py): as linhas `# MAGIC` são comentários,
então `%run` e `%pip` são ignorados aqui e este script carrega os arquivos na ordem certa,
todos no mesmo namespace (como o `%run` faria no Databricks).

Uso:
    python scripts/rodar_local.py

Requer Java 8/11/17 e `pip install -r requirements.txt`.
"""
import json
import os
import sys
import time
import urllib.request

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
NOTEBOOKS = ["00_config.py", "funcoes.py", "01_ingestao.py", "02_processamento_spark.py", "03_chunking.py"]

os.environ["RAG_SPARK_REPO"] = REPO
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402

spark = (
    SparkSession.builder.master("local[4]")
    .appName("RAG-SPARK")
    .config("spark.driver.memory", "3g")
    .config("spark.ui.port", "4050")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

namespace = {"__name__": "__main__", "spark": spark}
inicio = time.perf_counter()
for nome in NOTEBOOKS:
    caminho = os.path.join(REPO, "notebooks", nome)
    print(f"\n{'=' * 70}\n▶ {nome}\n{'=' * 70}")
    with open(caminho, encoding="utf-8") as arquivo:
        exec(compile(arquivo.read(), caminho, "exec"), namespace)
print(f"\nTotal: {time.perf_counter() - inicio:.1f}s")

# Evidência do shuffle: bytes escritos/lidos por estágio, direto da API REST da Spark UI
try:
    base = spark.sparkContext.uiWebUrl + "/api/v1/applications"
    app_id = spark.sparkContext.applicationId
    with urllib.request.urlopen(f"{base}/{app_id}/stages") as resposta:
        estagios = json.load(resposta)
    com_shuffle = [
        {
            "stage": s["stageId"],
            "nome": s["name"][:80],
            "tarefas": s["numTasks"],
            "shuffle_write_bytes": s["shuffleWriteBytes"],
            "shuffle_read_bytes": s["shuffleReadBytes"],
            "duracao_ms": s.get("executorRunTime"),
        }
        for s in estagios
        if s["status"] == "COMPLETE" and (s["shuffleWriteBytes"] or s["shuffleReadBytes"])
    ]
    destino = os.path.join(REPO, "docs", "resultados", "shuffle_local.json")
    with open(destino, "w", encoding="utf-8") as arquivo:
        json.dump(sorted(com_shuffle, key=lambda s: s["stage"]), arquivo, indent=2, ensure_ascii=False)
    total_w = sum(s["shuffle_write_bytes"] for s in com_shuffle)
    print(f"{len(com_shuffle)} estágios com shuffle, {total_w / 1e6:.1f} MB escritos → {destino}")
except Exception as erro:
    print(f"Não consegui ler a Spark UI: {erro}")

spark.stop()
