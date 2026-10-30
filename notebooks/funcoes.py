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
    import pymupdf as fitz  # import local: a UDF roda nos executores

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
    import pymupdf as fitz

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

# O NFKC troca "º" por "o", então "Nº" chega como "No"
_NUM = r"N\s*[ºOo°]\.?\s*(\d+)"
_TIPOS = [
    ("Portaria Interministerial", r"PORTARIA\s+INTERMINISTERIAL[A-Z/ ]{0,20}?" + _NUM + r"\s*,?\s*DE\s+\d{1,2}\s+DE\s+\w+\s+DE\s+(\d{4})"),
    ("Resolução CIF", r"RESOLU[ÇC][ÃA]O\s+CIF\s+" + _NUM + r"\s*,?\s*DE\s+\d{1,2}\s+DE\s+\w+\s+DE\s+(\d{4})"),
    ("Nota Técnica Conjunta", r"NOTA\s+T[ÉE]CNICA\s+CONJUNTA\s+" + _NUM + r"\s*/\s*(\d{4})"),
    ("Nota Técnica", r"NOTA\s+T[ÉE]CNICA\s+" + _NUM + r"\s*/\s*(\d{4})"),
]

_ORGAO_POR_TIPO = {"Resolução CIF": "CIF", "Portaria Interministerial": "MEC/MF"}


def _orgao(tipo: str, titulo: str, texto: str):
    if tipo in _ORGAO_POR_TIPO:
        return _ORGAO_POR_TIPO[tipo]
    # Notas técnicas: o órgão aparece no cabeçalho ou no próprio número ("4/2025/CGMEB/DAEB- INEP")
    if "INEP" in titulo or texto[:200].upper().startswith("INSTITUTO NACIONAL"):
        return "INEP"
    if "SEB" in titulo or texto[:200].upper().startswith("MINIST"):
        return "MEC/SEB"
    return None


def _titulo(texto: str):
    """Acha o título do documento: o primeiro tipo de norma que aparece no texto.

    1ª passada só com maiúsculas, porque os títulos vêm assim e as citações no corpo não
    (ex.: decisões do CADE no DOU citam "Nota Técnica nº ..."). 2ª passada em qualquer caixa.
    """
    for flags in (0, re.I):
        achados = [(m.start(), nome, m) for nome, padrao in _TIPOS for m in [re.search(padrao, texto, flags)] if m]
        if achados:
            _, nome, m = min(achados, key=lambda achado: achado[0])
            return nome, m
    return "Outro", None


def _romanos(trecho: str) -> list:
    achados = set()
    for grupo in re.findall(r"\b([IVX]+(?:\s*(?:,|e)\s*[IVX]+)*)\b", trecho):
        achados.update(r for r in re.split(r"\s*(?:,|e)\s*", grupo) if r in ROMANOS)
    return sorted(achados, key=ROMANOS.index)


def extrair_metadados(nome_arquivo: str, primeira_pagina: str) -> dict:
    texto = normalizar_espacos(unicodedata.normalize("NFKC", primeira_pagina or ""))

    tipo, m = _titulo(texto)
    numero = int(m.group(1)) if m else None
    ano = int(m.group(2)) if m else None
    titulo = texto[m.start():m.end() + 40].upper() if m else ""

    orgao = _orgao(tipo, titulo, texto)

    processo = re.search(r"\d{5}\.\d{6}/\d{4}-\d{2}", texto)
    publicacao = re.search(r"Publicado em:\s*(\d{2}/\d{2}/\d{4})", texto)

    # Assunto: notas técnicas têm a seção ASSUNTO; resoluções e portarias, a ementa logo após o título
    sec = re.search(r"ASSUNTO\s*(?:1\s*\.\s*1\s*\.\s*)?(.+?)(?=\s(?:\d\s*\.\s*)?(?:REFER[ÊE]NCIAS|SUM[ÁA]RIO|CONTEXTO|INTRODU)|$)", texto)
    if sec:
        assunto = sec.group(1)
    else:
        resto = texto[m.end():] if m else texto
        assunto = re.split(r"\s(?:A COORDENADORA|O MINISTRO|OS MINISTROS|RESOLVE)", resto, maxsplit=1)[0]
    assunto = assunto[:600].strip()

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chunking

# COMMAND ----------


def dividir_em_chunks(texto: str, tamanho: int, sobreposicao: int, minimo: int = 50) -> list:
    """Janela deslizante de `tamanho` caracteres com `sobreposicao`, sem cortar palavras.

    Mesma ideia do `chunk_text_func` do notebook da disciplina, mas o corte cai no último
    espaço ou quebra de linha antes do limite, e o próximo chunk começa no início de uma palavra.
    """
    assert 0 <= sobreposicao < tamanho // 2, "a sobreposição precisa ser menor que metade do chunk"
    texto = (texto or "").strip()
    chunks = []
    inicio = 0
    while inicio < len(texto):
        fim = min(inicio + tamanho, len(texto))
        if fim < len(texto):
            corte = max(texto.rfind(" ", inicio + tamanho // 2, fim), texto.rfind("\n", inicio + tamanho // 2, fim))
            if corte > inicio:
                fim = corte
        trecho = texto[inicio:fim].strip()
        if len(trecho) >= minimo:
            chunks.append(trecho)
        if fim >= len(texto):
            break
        # Volta `sobreposicao` caracteres e avança até o começo da próxima palavra
        proximo = fim - sobreposicao
        while proximo < fim and not texto[proximo - 1].isspace():
            proximo += 1
        inicio = proximo
    return chunks
