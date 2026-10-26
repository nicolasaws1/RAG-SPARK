# RAG-SPARK

Pipeline de **Retrieval-Augmented Generation** sobre normas da complementação **VAAR do Fundeb**, com o processamento dos PDFs distribuído em **Apache Spark** (Databricks).

**Integrantes:** Ricardo Henrique Guimarães · Nicolas Alves Witzel da Silva · Flavio Vieira Araujo · Gustavo Romão Tiedra · Emerson Aparecido Si

## Cronograma

| Semana | Prazo | Etapa | Status | Onde |
|---|---|---|---|---|
| 1 | 19/10/2026 | Ideia e definição do problema | ✅ (falta o 10º PDF) | [`docs/01-proposta.md`](docs/01-proposta.md), [`docs/02-arquitetura.md`](docs/02-arquitetura.md) |
| 2 | 26/10/2026 | Ingestão e preparação dos PDFs | ✅ código pronto, falta rodar no Databricks | [`notebooks/01_ingestao.py`](notebooks/01_ingestao.py) |
| 3 | 02/11/2026 | Spark e processamento distribuído | ✅ código pronto, falta rodar no Databricks | [`notebooks/02_processamento_spark.py`](notebooks/02_processamento_spark.py) |
| 4 | 09/11/2026 | Construção do RAG | ⏳ | |
| 5 | 16/11/2026 | LLM e agentes de IA | ⏳ | |
| 6 | 23/11/2026 | Avaliação, integração e melhoria | ⏳ | |
| 7 | 30/11/2026 | Conclusão e apresentação final | ⏳ | |

## Estrutura

```
data/pdfs/                    PDFs do conjunto de teste
docs/                         proposta, arquitetura e resultados
notebooks/
  00_config.py                caminhos, parâmetros, salvar/ler tabelas, medição de tempo
  funcoes.py                  extração de PDF e metadados (Python puro, testável)
  01_ingestao.py              semana 2: PDFs → tabelas documentos e paginas
  02_processamento_spark.py   semana 3: limpeza, shuffle, joins e métricas
scripts/rodar_local.py        roda os notebooks fora do Databricks
legado/                       versão anterior do notebook (1 PDF, TinyLlama)
```

Os notebooks estão no formato *Databricks source* (`.py` com `# COMMAND ----------`): o Databricks abre como notebook e, fora dele, são arquivos Python comuns.

## Como rodar no Databricks Free Edition

1. Em **Workspace → Create → Git folder**, cole a URL deste repositório.
2. Abra `notebooks/01_ingestao.py` e clique em **Run all**. Ele cria o schema `workspace.rag_spark`, o volume `pdfs`, copia os PDFs do repositório para o volume e gera as tabelas `documentos` e `paginas`.
3. Abra `notebooks/02_processamento_spark.py` e clique em **Run all**.
4. **Evidência do shuffle:** abaixo de cada célula, clique em **See performance → Query profile**. Os operadores `Exchange`/`Shuffle` mostram linhas e bytes trocados. Os planos físicos também ficam impressos na saída das células.
5. As tabelas ficam em **Catalog → workspace → rag_spark** e podem ser consultadas em SQL:

```sql
SELECT tipo, numero, ano, orgao, exercicio_vaar, condicionalidades
FROM workspace.rag_spark.documentos
ORDER BY exercicio_vaar, tipo;
```

## Como rodar localmente

Requer Java 8, 11 ou 17 e Python 3.10 ou 3.11. **No Windows, o PySpark 3.5 não funciona com Python 3.12**: o processo Python que executa as tarefas fecha sozinho ("Python worker exited unexpectedly"). Também no Windows, o Spark não consegue listar pastas sem o `winutils`, então o notebook 01 lê os PDFs pelo Python e monta o mesmo DataFrame que o `binaryFile` geraria. No Databricks nada disso acontece.

```bash
pip install -r requirements.txt
```

```bash
python scripts/rodar_local.py
```

Os tempos, os planos de execução e os bytes de shuffle de cada estágio são gravados em `docs/resultados/`.
