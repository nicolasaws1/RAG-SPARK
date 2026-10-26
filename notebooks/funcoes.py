# Databricks notebook source
# MAGIC %md
# MAGIC # Funções do pipeline (Python puro)
# MAGIC
# MAGIC Tudo aqui é Python sem Spark, para poder ser testado com `pytest` e usado dentro das UDFs.
# MAGIC Os notebooks `01` e `02` carregam este arquivo com `%run ./funcoes`.

# COMMAND ----------

import hashlib
import re
import unicodedata

ROMANOS = ["I", "II", "III", "IV", "V"]


def normalizar_espacos(texto: str) -> str:
    return re.sub(r"\s+", " ", texto or "").strip()


def sha256_bytes(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura de PDF (PyMuPDF)

# COMMAND ----------


def info_pdf(conteudo: bytes) -> dict:
    """Número de páginas, hash e texto da primeira página (usado nos metadados)."""
    import fitz  # import local: a UDF roda nos executores

    with fitz.open(stream=conteudo, filetype="pdf") as doc:
        primeira = doc[0].get_text("text") if doc.page_count else ""
        return {
            "n_paginas": doc.page_count,
            "sha256": sha256_bytes(conteudo),
            "primeira_pagina": primeira,
        }


def faixas_de_paginas(n_paginas: int, tamanho: int) -> list:
    """Divide [1, n] em faixas de `tamanho` páginas. Cada faixa vira uma tarefa Spark."""
    return [
        {"pag_ini": ini, "pag_fim": min(ini + tamanho - 1, n_paginas)}
        for ini in range(1, n_paginas + 1, tamanho)
    ]


def extrair_paginas(conteudo: bytes, pag_ini: int, pag_fim: int, detectar_tabelas: bool = True) -> list:
    """Extrai texto e tabelas das páginas [pag_ini, pag_fim] (1-based, inclusivo)."""
    import fitz

    saida = []
    with fitz.open(stream=conteudo, filetype="pdf") as doc:
        for numero in range(pag_ini, pag_fim + 1):
            pagina = doc[numero - 1]
            tabelas = []
            if detectar_tabelas:
                try:
                    for tab in pagina.find_tables().tables:
                        md = tab.to_markdown(clean=True)
                        if md and md.count("|") > 4:
                            tabelas.append(md)
                except Exception:
                    pass  # find_tables falha em páginas malformadas; o texto continua valendo
            saida.append({
                "pagina": numero,
                "texto": pagina.get_text("text"),
                "n_tabelas": len(tabelas),
                "tabelas": tabelas,
            })
    return saida

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metadados a partir da primeira página

# COMMAND ----------

_TIPOS = [
    ("Portaria Interministerial", r"PORTARIA\s+INTERMINISTERIAL[^\n]*?N[ºo°]\s*(\d+)\s*,?\s*DE\s+\d{1,2}\s+DE\s+\w+\s+DE\s+(\d{4})"),
    ("Resolução CIF", r"RESOLU[ÇC][ÃA]O\s+CIF\s+N[ºo°]\s*(\d+)\s*,?\s*DE\s+\d{1,2}\s+DE\s+\w+\s+DE\s+(\d{4})"),
    ("Nota Técnica Conjunta", r"NOTA\s+T[ÉE]CNICA\s+CONJUNTA\s+N[ºo°]\s*(\d+)\s*/\s*(\d{4})"),
    ("Nota Técnica", r"NOTA\s+T[ÉE]CNICA\s+N[ºo°]\s*(\d+)\s*/\s*(\d{4})"),
]

_ORGAOS = [
    ("INEP", r"INSTITUTO NACIONAL DE ESTUDOS E PESQUISAS|INEP"),
    ("CIF", r"RESOLU[ÇC][ÃA]O\s+CIF"),
    ("MEC/MF", r"INTERMINISTERIAL\s+MEC/MF"),
    ("MEC/SEB", r"Secretaria de Educa[çc][ãa]o B[áa]sica|/SEB"),
]


def _romanos(trecho: str) -> list:
    achados = set()
    for grupo in re.findall(r"\b([IVX]+(?:\s*(?:,|e)\s*[IVX]+)*)\b", trecho):
        achados.update(r for r in re.split(r"\s*(?:,|e)\s*", grupo) if r in ROMANOS)
    return sorted(achados, key=ROMANOS.index)


def extrair_metadados(nome_arquivo: str, primeira_pagina: str) -> dict:
    texto = normalizar_espacos(unicodedata.normalize("NFKC", primeira_pagina or ""))
    upper = texto.upper()

    tipo, numero, ano = "Outro", None, None
    for nome_tipo, padrao in _TIPOS:
        m = re.search(padrao, upper)
        if m:
            tipo, numero, ano = nome_tipo, int(m.group(1)), int(m.group(2))
            break

    orgao = next((o for o, p in _ORGAOS if re.search(p, texto, flags=re.I if o != "INEP" else 0)), None)

    processo = re.search(r"\d{5}\.\d{6}/\d{4}-\d{2}", texto)
    publicacao = re.search(r"Publicado em:\s*(\d{2}/\d{2}/\d{4})", texto)

    # Assunto: notas técnicas têm a seção ASSUNTO; resoluções e portarias, a ementa logo após o título
    m = re.search(r"ASSUNTO\s*(?:1\s*\.\s*1\s*\.\s*)?(.+?)(?=\s(?:\d\s*\.\s*)?(?:REFER[ÊE]NCIAS|SUM[ÁA]RIO|CONTEXTO|INTRODU)|$)", texto)
    if not m:
        m = re.search(r"DE \d{4}\s+(.+?)(?=\s(?:A COORDENADORA|O MINISTRO|OS MINISTROS|RESOLVE)|$)", texto)
    assunto = (m.group(1) if m else texto)[:600].strip()

    exercicios = [int(a) for a in re.findall(r"exerc[íi]cio(?:\s+financeiro)?\s+de\s+(\d{4})", assunto, flags=re.I)]
    condicionalidades = set()
    for trecho in re.findall(r"[Cc]ondicionalidades?\s+((?:[IVX]+)(?:\s*(?:,|e)\s*[IVX]+)*)", assunto):
        condicionalidades.update(_romanos(trecho))
    if re.search(r"condicionalidade", assunto, flags=re.I):
        for trecho in re.findall(r"incisos?\s+((?:[IVX]+)(?:\s*(?:,|e)\s*[IVX]+)*)", assunto, flags=re.I):
            condicionalidades.update(_romanos(trecho))

    return {
        "arquivo": nome_arquivo,
        "tipo": tipo,
        "numero": numero,
        "ano": ano,
        "orgao": orgao,
        "processo_sei": processo.group(0) if processo else None,
        "data_publicacao": publicacao.group(1) if publicacao else None,
        "exercicio_vaar": max(exercicios) if exercicios else None,
        "condicionalidades": sorted(condicionalidades, key=ROMANOS.index),
        "assunto": assunto,
    }
