# Entrega · Execução até a limpeza (passo 2) e o chunking (passo 3)

Referência: `Projeto RAG - Spark/rag_e_chat_pipeline_spark.ipynb` (GitLab, INFRA_BIG_DATA / Exercícios).

O notebook da disciplina processa um PDF por vez, em um único computador. Aqui as mesmas etapas rodam no **Databricks (Spark serverless)** sobre os 9 PDFs do projeto:

| Passo do notebook da disciplina | Onde está neste repositório | Tabela gerada |
|---|---|---|
| Aula 1 · Ingestão de dados | [`notebooks/01_ingestao.py`](../notebooks/01_ingestao.py) | `documentos`, `paginas` |
| Aula 2 · Limpeza de dados | [`notebooks/02_processamento_spark.py`](../notebooks/02_processamento_spark.py), seção 1 | `paginas_limpas` |
| Aula 3 · Chunking | [`notebooks/03_chunking.py`](../notebooks/03_chunking.py) | `chunks` |

## Diferenças em relação ao notebook da disciplina

| | Notebook da disciplina | Este projeto |
|---|---|---|
| Leitura do PDF | `pypdf` no driver, um arquivo | `binaryFile` + PyMuPDF em UDF, 9 arquivos, em faixas de 20 páginas por tarefa |
| Limpeza | UDF Python; põe tudo em minúsculas e apaga `º`, `ª`, `§` | funções nativas do Spark; mantém maiúsculas e símbolos das referências legais ("art. 14, § 1º"), remove carimbo do DOU, ligaturas e hifenização |
| Chunking | 300 caracteres, corta no meio da palavra | 800 caracteres com 150 de sobreposição, corta no último espaço; tabelas viram chunks próprios |
| Identificador do chunk | `monotonically_increasing_id()` | `doc_id-pagina-tipo-indice`, igual em toda execução |
| Saída | Parquet em `/tmp` | tabelas Delta em `workspace.rag_spark` |

## Capturas de tela

Salve cada print em [`docs/prints/`](prints) com o nome indicado; as imagens aparecem aqui automaticamente.

### Passo 1 · Ingestão (`01_ingestao`)

**1a.** Saída da última célula da seção 6: a tabela de conferência com `ok = true` e o texto `📄 Ingestão concluída | Documentos: ... | Páginas: ...`, seguido da lista dos documentos com tipo, número, órgão, exercício e condicionalidades.

![Ingestão concluída](prints/01-ingestao.png)

**1b.** Seção 4: a tabela de distribuição das faixas por partição (mostra a Portaria dividida em várias tarefas).

![Faixas por partição](prints/01-faixas-particoes.png)

### Passo 2 · Limpeza (`02_processamento_spark`, seção 1)

**2a.** Saída da célula de limpeza: `🧹 Limpeza concluída | Páginas: ... | Caracteres: ... → ... (-x%)` e a amostra com o início do texto de cada página.

![Limpeza concluída](prints/02-limpeza.png)

**2b.** (Opcional, mostra o Spark trabalhando.) **See performance → Query profile** da célula da seção 2 (`groupBy`), onde aparece o shuffle.

![Query profile do groupBy](prints/02-query-profile-shuffle.png)

### Passo 3 · Chunking (`03_chunking`)

**3a.** Saída da seção 3: `✂️ Chunking concluído | Total de chunks: ...`, com os totais de chunks de texto e de tabela e os tamanhos mínimo, médio e máximo.

![Chunking concluído](prints/03-chunking.png)

**3b.** Seção 4: chunks por documento.

![Chunks por documento](prints/03-chunks-por-documento.png)

**3c.** Seção 5: dois chunks seguidos da mesma página, mostrando a sobreposição.

![Sobreposição entre chunks](prints/03-sobreposicao.png)

**3d.** Seção 6: amostra da tabela `chunks` com os metadados.

![Tabela chunks](prints/03-tabela-chunks.png)

## Consulta de conferência (SQL Editor)

```sql
SELECT tipo_chunk, COUNT(*) AS chunks, ROUND(AVG(chunk_len)) AS tamanho_medio
FROM workspace.rag_spark.chunks
GROUP BY tipo_chunk;
```
