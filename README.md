# RAG-SPARK

Pipeline de **Retrieval-Augmented Generation** sobre normas da complementação **VAAR do Fundeb**, com o processamento dos PDFs distribuído em **Apache Spark** (Databricks).

**Integrantes:** Ricardo Henrique Guimarães · Nicolas Alves Witzel da Silva · Flavio Vieira Araujo · Gustavo Romão Tiedra · Emerson Aparecido Si

## Cronograma

| Semana | Prazo | Etapa | Status | Onde |
|---|---|---|---|---|
| 1 | 19/10/2026 | Ideia e definição do problema | ✅ (falta o 10º PDF) | [`docs/01-proposta.md`](docs/01-proposta.md), [`docs/02-arquitetura.md`](docs/02-arquitetura.md) |
| 2 | 26/10/2026 | Ingestão e preparação dos PDFs | ✅ executado no Databricks | [`notebooks/01_ingestao.py`](notebooks/01_ingestao.py) |
| 3 | 02/11/2026 | Spark e processamento distribuído | ✅ executado no Databricks | [`notebooks/02_processamento_spark.py`](notebooks/02_processamento_spark.py) |
| 4 | 09/11/2026 | Construção do RAG | 🔄 chunking pronto; faltam embeddings e índice | [`notebooks/03_chunking.py`](notebooks/03_chunking.py) |
| 5 | 16/11/2026 | LLM e agentes de IA | ⏳ | |
| 6 | 23/11/2026 | Avaliação, integração e melhoria | ⏳ | |
| 7 | 30/11/2026 | Conclusão e apresentação final | ⏳ | |

**Entrega do exercício "execução até a limpeza (passo 2) e o chunking (passo 3)":** [`docs/03-entrega-limpeza-chunking.md`](docs/03-entrega-limpeza-chunking.md), com a correspondência entre o notebook da disciplina e este repositório e as capturas de tela.

## Estrutura

```
data/pdfs/                    PDFs do conjunto de teste
docs/                         proposta, arquitetura, entrega e resultados
docs/prints/                  capturas de tela da execução no Databricks
notebooks/
  00_config.py                caminhos, parâmetros, salvar/ler tabelas, medição de tempo
  funcoes.py                  extração de PDF, metadados e chunking (Python puro, testável)
  01_ingestao.py              semana 2: PDFs → tabelas documentos e paginas
  02_processamento_spark.py   semana 3: limpeza, shuffle, joins e métricas
  03_chunking.py              paginas_limpas → tabela chunks (texto e tabelas)
scripts/rodar_local.py        roda os notebooks fora do Databricks
legado/                       versão anterior do notebook (1 PDF, TinyLlama)
```

Os notebooks estão no formato *Databricks source* (`.py` com `# COMMAND ----------`): o Databricks abre como notebook e, fora dele, são arquivos Python comuns.

## Como rodar no Databricks Free Edition

**Ambiente:** computador *Serverless*, ambiente versão 6 (já fixado no cabeçalho dos notebooks 01, 02 e 03). Não precisa criar cluster.

1. Crie uma conta gratuita em [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition).
2. Em **Workspace → Home → Create → Git folder**, cole `https://github.com/nicolasaws1/RAG-SPARK` e clique em **Create Git folder**. O repositório é público, então não precisa de token.
3. Abra `notebooks/01_ingestao`, confira se o computador selecionado é **Serverless** e clique em **Run all**.
   - A célula `%pip install pymupdf` reinicia o Python; é esperado.
   - Ele cria o schema `workspace.rag_spark` e o volume `pdfs`, copia os PDFs do repositório para o volume e gera as tabelas `inventario`, `documentos`, `paginas` e `metricas_ingestao`.
   - A extração da Portaria (169 páginas, com detecção de tabelas) é a célula mais demorada.
   - No fim, a tabela de conferência deve mostrar `ok = true` para todos os PDFs.
4. Abra `notebooks/02_processamento_spark` e clique em **Run all**. Ele gera `paginas_limpas`, `texto_por_doc` e `metricas`.
5. Abra `notebooks/03_chunking` e clique em **Run all**. Ele gera `chunks` e `metricas_chunking`. Tamanho e sobreposição dos chunks ficam em `00_config` (`CHUNK_SIZE`, `CHUNK_OVERLAP`).
6. **Evidência do shuffle:** abaixo de cada célula, clique em **See performance → Query profile**. Os operadores `Exchange`/`Shuffle` mostram linhas e bytes trocados. Os planos físicos e a contagem de shuffles de cada join também ficam impressos na saída da seção 3 do notebook 02.
7. As tabelas ficam em **Catalog → workspace → rag_spark** e podem ser consultadas no **SQL Editor**:

```sql
SELECT tipo, numero, ano, orgao, exercicio_vaar, condicionalidades
FROM workspace.rag_spark.documentos
ORDER BY exercicio_vaar, tipo;
```

```sql
SELECT etapa, segundos, detalhes FROM workspace.rag_spark.metricas_ingestao
UNION ALL
SELECT etapa, segundos, detalhes FROM workspace.rag_spark.metricas
UNION ALL
SELECT etapa, segundos, detalhes FROM workspace.rag_spark.metricas_chunking;
```

Para atualizar o código no Databricks depois de um push, abra o Git folder e clique em **Pull**.

## Como rodar localmente

Requer Java 8, 11 ou 17 e Python 3.10 ou 3.11. **No Windows, o PySpark 3.5 não funciona com Python 3.12**: o processo Python que executa as tarefas fecha sozinho ("Python worker exited unexpectedly"). Também no Windows, o Spark não consegue listar pastas sem o `winutils`, então o notebook 01 lê os PDFs pelo Python e monta o mesmo DataFrame que o `binaryFile` geraria. No Databricks nada disso acontece.

```bash
pip install -r requirements.txt
```

```bash
python scripts/rodar_local.py
```

Os tempos, os planos de execução e os bytes de shuffle de cada estágio são gravados em `docs/resultados/`.
