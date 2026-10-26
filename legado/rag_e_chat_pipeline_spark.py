# Databricks notebook source
# MAGIC %md
# MAGIC Integrantes do Grupo
# MAGIC
# MAGIC Ricardo Henrique Guimarães
# MAGIC Nicolas Alves Witzel da Silva
# MAGIC Flavio Vieira Araujo
# MAGIC Gustavo Romão Tiedra
# MAGIC Emerson Aparecido Si

# COMMAND ----------

# MAGIC %md
# MAGIC # 🔍 Pipeline RAG com Apache Spark & Chat Interativo
# MAGIC
# MAGIC Pipeline completo de **Retrieval-Augmented Generation** sobre PDF, dividido em 7 etapas:
# MAGIC
# MAGIC | Aula | Etapa |
# MAGIC |------|-------|
# MAGIC | 1 | Ingestão de Dados |
# MAGIC | 2 | Limpeza de Dados |
# MAGIC | 3 | Chunking |
# MAGIC | 4 | Embeddings |
# MAGIC | 5 | Busca Vetorial |
# MAGIC | 6 | Pipeline RAG Base |
# MAGIC | 7 | Chat Interativo com Histórico |
# MAGIC
# MAGIC **Libs gratuitas:** PySpark · pypdf · sentence-transformers · faiss-cpu · numpy · transformers · ipywidgets
# MAGIC
# MAGIC > Testado no container `jupyter/pyspark-notebook`. Requer Java 11+ (já incluso na imagem).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📦 Instalação de Dependências
# MAGIC
# MAGIC Execute esta célula uma única vez. O kernel será reiniciado automaticamente após a instalação.

# COMMAND ----------

import subprocess, sys

pkgs = [
    "pypdf",                  # Leitura de PDF — gratuito
    "sentence-transformers",  # Embeddings locais — gratuito (Apache 2.0)
    "faiss-cpu",              # Vector store local — gratuito (MIT)
    "numpy",                  # Álgebra linear — gratuito
    "transformers",           # Modelos de LLM (Hugging Face)
    "torch",                  # Backend de Deep Learning
    "ipywidgets",             # Interface de chat interativa
]

for pkg in pkgs:
    print(f"Instalando {pkg}...")
    r = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", pkg],
        capture_output=True, text=True
    )
    status = "✅" if r.returncode == 0 else f"❌ {r.stderr[:80]}"
    print(f"  {status} {pkg}")

print("\n✅ Dependências instaladas — reinicie o kernel se esta for a primeira execução.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚙️ Configuração Global
# MAGIC
# MAGIC Defina o caminho do PDF e os parâmetros do pipeline aqui.

# COMMAND ----------

import os, warnings
warnings.filterwarnings("ignore")

# ─── Ajuste conforme necessário ───────────────────────────────
PDF_PATH       = "/home/jovyan/trabalho/documento.pdf"  # << seu PDF aqui
CHUNK_SIZE     = 300    # caracteres por chunk (Aula 3)
CHUNK_OVERLAP  = 50     # sobreposição entre chunks
EMBED_MODEL    = "all-MiniLM-L6-v2"  # modelo de embedding (80 MB, gratuito)
TOP_K          = 3      # resultados retornados na busca
OUTPUT_DIR     = "/tmp/rag_output"  # diretório de saída
# ─────────────────────────────────────────────────────────────

os.makedirs(OUTPUT_DIR, exist_ok=True)
print("Configuração carregada ✅")
print(f"  PDF         : {PDF_PATH}")
print(f"  Chunk size  : {CHUNK_SIZE} chars | Overlap: {CHUNK_OVERLAP}")
print(f"  Modelo Emb  : {EMBED_MODEL}")
print(f"  Saída       : {OUTPUT_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🧩 Aula 1 — Ingestão de Dados

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (
    SparkSession.builder
    .appName(
        "RAG-Pipeline"
    )
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(f"Spark {spark.version} iniciado ✅")

from pypdf import PdfReader

if not os.path.exists(PDF_PATH):
    print(f"\n⚠️  '{PDF_PATH}' não encontrado. Criando PDF de demonstração...")
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import A4
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "reportlab"])
        from reportlab.pdfgen import canvas
        from reportlab.lib.pagesizes import A4

    demo_pages = [
        ("Apache Spark — Visão Geral",
         "Apache Spark é um motor de processamento distribuído de dados em larga escala. "
         "Spark suporta SQL, streaming, machine learning e grafos. "
         "Foi criado na UC Berkeley em 2009 e cedido à Apache Foundation em 2013. "
         "Sua principal vantagem é processar dados em memória, sendo muito mais rápido que MapReduce."),
        ("Arquitetura do Spark",
         "A arquitetura é baseada em modelo driver-executor. "
         "O Driver coordena o job e os Executors processam as partições dos dados. "
         "O DAGScheduler otimiza o plano de execução em estágios. "
         "O Cluster Manager pode ser YARN, Kubernetes ou Spark Standalone."),
        ("DataFrames e Spark SQL",
         "DataFrames são coleções distribuídas com schema definido. "
         "O otimizador Catalyst transforma queries em planos de execução eficientes. "
         "Spark SQL permite consultar dados com sintaxe ANSI SQL. "
         "Particionamento e cache são técnicas fundamentais de performance."),
        ("Delta Lake",
         "Delta Lake adiciona transações ACID ao Spark. "
         "Suporta operações de upsert, delete e time travel com versionamento. "
         "Schema enforcement previne dados corrompidos no data lake. "
         "É amplamente usado em arquiteturas Lakehouse no Databricks."),
        ("RAG com Spark",
         "Retrieval-Augmented Generation combina busca semântica com modelos de linguagem. "
         "Com Spark é possível processar grandes volumes de documentos de forma distribuída. "
         "O pipeline envolve ingestão, limpeza, chunking, embeddings e busca vetorial. "
         "FAISS é uma biblioteca eficiente para indexação e busca de vetores densos."),
        ("Machine Learning Distribuído",
         "MLlib é a biblioteca de ML distribuído do Spark. "
         "Suporta classificação, regressão, clustering e sistemas de recomendação. "
         "Pipelines ML encadeiam transformações e estimadores de forma declarativa. "
         "O modelo pode ser exportado e servido via MLflow ou outras plataformas."),
    ]

    os.makedirs(os.path.dirname(PDF_PATH), exist_ok=True)
    c = canvas.Canvas(PDF_PATH, pagesize=A4)
    _, h = A4
    for title, body in demo_pages:
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, h - 80, title)
        c.setFont("Helvetica", 11)
        y = h - 115
        for sent in body.split(". "):
            c.drawString(50, y, sent.strip() + ".")
            y -= 22
        c.showPage()
    c.save()
    print(f"  PDF de demonstração criado: {PDF_PATH} ✅")

reader     = PdfReader(PDF_PATH)
pages_data = []
for i, page in enumerate(reader.pages):
    text = (page.extract_text() or "").strip()
    if text:
        pages_data.append({"page_id": i + 1, "page_text": text,
                            "source": os.path.basename(PDF_PATH)})

schema = StructType([
    StructField("page_id",   IntegerType(), False),
    StructField("page_text", StringType(),  True),
    StructField("source",    StringType(),  True),
])

raw_df = spark.createDataFrame(pages_data, schema=schema)
raw_path = f"{OUTPUT_DIR}/raw_documents"
raw_df.write.mode("overwrite").parquet(raw_path)

print(f"\n📄 Ingestão concluída | Páginas: {raw_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🧩 Aula 2 — Limpeza de Dados

# COMMAND ----------

import re
from pyspark.sql.functions import udf, col, length

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^a-záàãâéêíóôõúüç0-9 .,;:!?()/\-]', '', text)
    return text.strip()

clean_udf = udf(clean_text, StringType())
raw_df = spark.read.parquet(f"{OUTPUT_DIR}/raw_documents")

clean_df = (
    raw_df
    .withColumn("clean_text", clean_udf(col("page_text")))
    .withColumn("raw_len",   length(col("page_text")))
    .withColumn("clean_len", length(col("clean_text")))
    .filter(col("clean_len") > 10)
    .select("page_id", "source", "clean_text", "raw_len", "clean_len")
)

clean_path = f"{OUTPUT_DIR}/clean_documents"
clean_df.write.mode("overwrite").parquet(clean_path)
print("🧹 Limpeza concluída ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🧩 Aula 3 — Chunking

# COMMAND ----------

from pyspark.sql.functions import posexplode, monotonically_increasing_id
from pyspark.sql.types import ArrayType

def chunk_text_func(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP):
    chunks = []
    i = 0
    while i < len(text):
        chunks.append(text[i: i + size])
        i += size - overlap
    return [c for c in chunks if len(c.strip()) > 20]

chunk_udf = udf(lambda t: chunk_text_func(t, CHUNK_SIZE, CHUNK_OVERLAP), ArrayType(StringType()))
clean_df = spark.read.parquet(f"{OUTPUT_DIR}/clean_documents")

chunks_df = (
    clean_df
    .withColumn("chunks", chunk_udf(col("clean_text")))
    .select("page_id", "source", posexplode(col("chunks")).alias("chunk_index", "chunk_text"))
    .withColumn("chunk_id", monotonically_increasing_id())
    .withColumn("chunk_len", length(col("chunk_text")))
)

chunks_path = f"{OUTPUT_DIR}/chunks"
chunks_df.write.mode("overwrite").parquet(chunks_path)
print(f"✂️  Chunking concluído | Total de chunks: {chunks_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🧩 Aula 4 — Embeddings

# COMMAND ----------

import numpy as np
from sentence_transformers import SentenceTransformer

print(f"Carregando modelo de embedding '{EMBED_MODEL}'...")
model = SentenceTransformer(EMBED_MODEL)
chunks_df = spark.read.parquet(f"{OUTPUT_DIR}/chunks")
chunks_local = chunks_df.select("chunk_id", "page_id", "source", "chunk_index", "chunk_text").toPandas()

texts = chunks_local["chunk_text"].tolist()
embeddings = model.encode(texts, batch_size=32, show_progress_bar=True)
chunks_local["embedding"] = embeddings.tolist()
print(f"🔢 Embeddings gerados para {len(chunks_local)} chunks (Dim: {len(chunks_local['embedding'].iloc[0])})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🧩 Aula 5 — Busca Vetorial

# COMMAND ----------

import faiss

dim = len(chunks_local["embedding"].iloc[0])
matrix = np.array(chunks_local["embedding"].tolist(), dtype="float32")
faiss.normalize_L2(matrix)

index = faiss.IndexFlatIP(dim)
index.add(matrix)
faiss.write_index(index, f"{OUTPUT_DIR}/faiss.index")

def search_topk(query: str, k: int = TOP_K) -> list:
    q_vec = model.encode([query], normalize_embeddings=True).astype("float32")
    scores, indices = index.search(q_vec, k)
    results = []
    for score, idx in zip(scores[0], indices[0]):
        row = chunks_local.iloc[idx]
        results.append({
            "score": float(score),
            "page_id": int(row["page_id"]),
            "chunk_id": int(row["chunk_id"]),
            "text": row["chunk_text"],
        })
    return results

print("📦 Índice FAISS construído e pronto para buscas ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🧩 Aula 6 — Pipeline RAG Base & Chat Interativo com Histórico

# COMMAND ----------

from transformers import pipeline

# Carregando o TinyLlama otimizado para conversação em CPU local
CHAT_LLM_MODEL = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"

print(f"Carregando LLM de chat '{CHAT_LLM_MODEL}' (download único ~2.2GB)...")
chat_generator = pipeline(
    "text-generation",
    model=CHAT_LLM_MODEL,
    max_new_tokens=250,
    device=-1,
)
print("✅ LLM de chat pronto!")

# Histórico global da conversa
chat_history = []

def chat_rag(user_query: str, k: int = TOP_K) -> str:
    chunks_found = search_topk(user_query, k)
    context = "\n".join([f"[Trecho {i+1}] {r['text']}" for i, r in enumerate(chunks_found)])
    
    system_prompt = (
        "Você é um assistente virtual especialista no documento fornecido. "
        "Responda à pergunta do usuário utilizando estritamente o contexto abaixo. "
        "Se a resposta não estiver no contexto, informe educadamente que não encontrou a informação."
    )
    
    history_context = ""
    for h in chat_history[-4:]:
        history_context += f"<|user|>\n{h['user']}</s>\n<|assistant|>\n{h['assistant']}</s>\n"
        
    prompt = (
        f"<|system|>\n{system_prompt}\nContexto:\n{context}</s>\n"
        f"{history_context}"
        f"<|user|>\n{user_query}</s>\n"
        f"<|assistant|>"
    )
    
    outputs = chat_generator(
        prompt, 
        max_new_tokens=250, 
        do_sample=True, 
        temperature=0.7, 
        top_p=0.9
    )
    
    full_output = outputs[0]["generated_text"]
    if "<|assistant|>" in full_output:
        answer = full_output.split("<|assistant|>")[-1].strip()
    else:
        answer = full_output.replace(prompt, "").strip()
        
    chat_history.append({"user": user_query, "assistant": answer})
    return answer

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🏁 Interface Gráfica de Chat Interativo (`ipywidgets`)

# COMMAND ----------

import ipywidgets as widgets
from IPython.display import display, HTML, clear_output

chat_output = widgets.Output()
chat_input = widgets.Text(
    placeholder="Digite sua mensagem e pressione Enter...",
    layout=widgets.Layout(width="75%")
)
send_btn = widgets.Button(
    description="Enviar", 
    button_style="primary", 
    icon="comment"
)

messages_log = []

def render_chat():
    with chat_output:
        clear_output(wait=True)
        html = "<div style='border: 1px solid #cbd5e1; border-radius: 8px; padding: 15px; max-height: 450px; overflow-y: auto; background: #ffffff;'>"
        if not messages_log:
            html += "<p style='color: #64748b; text-align: center;'>Nenhuma mensagem ainda. Comece a conversar abaixo! 👇</p>"
        for msg in messages_log:
            if msg["role"] == "user":
                html += f"<div style='margin-bottom: 10px; text-align: right;'><span style='background: #3b82f6; color: white; padding: 8px 12px; border-radius: 12px; display: inline-block; max-width: 80%; text-align: left;'><b>Você:</b><br>{msg['text']}</span></div>"
            else:
                html += f"<div style='margin-bottom: 10px; text-align: left;'><span style='background: #f1f5f9; color: #1e293b; padding: 8px 12px; border-radius: 12px; display: inline-block; max-width: 80%;'><b>Assistente RAG:</b><br>{msg['text']}</span></div>"
        html += "</div>"
        display(HTML(html))

def handle_send(b_or_text):
    query = chat_input.value.strip()
    if not query:
        return
    messages_log.append({"role": "user", "text": query})
    chat_input.value = ""
    render_chat()
    
    answer = chat_rag(query)
    messages_log.append({"role": "assistant", "text": answer})
    render_chat()

send_btn.on_click(handle_send)
chat_input.on_submit(handle_send)

display(widgets.VBox([
    widgets.HTML("<h3 style='color: #1e293b;'>🤖 Chatbot RAG Local (Docker + Spark + TinyLlama)</h3>"),
    chat_output,
    widgets.HBox([chat_input, send_btn])
]))

render_chat()