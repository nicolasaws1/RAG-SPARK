# Semana 1 · Proposta do projeto

## Problema

As regras de distribuição da **complementação VAAR do Fundeb** (Valor Aluno Ano por Resultados, Lei nº 14.113/2020, art. 14) estão espalhadas em notas técnicas do INEP e do MEC, resoluções da Comissão Intergovernamental de Financiamento (CIF) e portarias publicadas no Diário Oficial. Cada condicionalidade (I a V) tem metodologia própria, que muda a cada exercício financeiro (2026, 2027).

Para saber, por exemplo, qual a participação mínima no Saeb exigida de uma rede de ensino, o gestor precisa descobrir qual documento trata daquela condicionalidade naquele exercício e ler o PDF inteiro.

## Objetivo

Construir um sistema de **RAG (Retrieval-Augmented Generation)** que responda perguntas em português sobre as condicionalidades do VAAR, **citando o documento e a página** de onde tirou a resposta. O processamento dos PDFs é feito com **Apache Spark** para que o mesmo pipeline escale de 10 para milhares de documentos.

## Público-alvo

- Secretarias municipais e estaduais de educação (equipes que preparam a documentação das condicionalidades);
- Conselhos de acompanhamento do Fundeb (CACS);
- Pesquisadores e jornalistas de financiamento da educação.

## Perguntas que o RAG deverá responder

| # | Pergunta | Tipo | Documento esperado |
|---|---|---|---|
| 1 | Qual o percentual mínimo de participação no Saeb exigido pela Condicionalidade II do VAAR? | factual | NT nº 4/2025 INEP |
| 2 | Quais critérios são usados na análise documental da Condicionalidade I para o exercício de 2026? | factual | NT nº 2/2025 SEB |
| 3 | O que mudou na Condicionalidade I entre os exercícios de 2026 e 2027? | comparativa | NT nº 2/2025 SEB e NT nº 3/2026 SEB |
| 4 | Como será aferida a Condicionalidade V no exercício de 2027? | factual | NT Conjunta nº 3/2026 INEP |
| 5 | Qual a diferença entre a metodologia da Condicionalidade V para 2026 e para 2027? | comparativa | NT Conjunta nº 14/2025 e NT Conjunta nº 3/2026 |
| 6 | Quais condicionalidades a Resolução CIF nº 15/2025 aprovou e para qual exercício? | factual | Resolução CIF nº 15/2025 |
| 7 | Qual resolução aprovou as metodologias das condicionalidades II e III? | metadados | Resolução CIF nº 17/2025 |
| 8 | Qual indicador de educação infantil foi aprovado para a complementação VAAT? | factual | Resolução CIF nº 17/2025 |
| 9 | Como as Condicionalidades I e IV são aferidas pelo INEP? | factual | NT nº 6/2025 INEP |
| 10 | O que estabelece a Portaria Interministerial MEC/MF nº 14/2025? | factual | Portaria MEC/MF nº 14/2025 |
| 11 | Quais documentos tratam do exercício de 2027? | metadados | todos com `exercicio_vaar = 2027` |
| 12 | Qual pergunta está fora do escopo? Ex.: "Qual o valor do piso salarial dos professores em 2026?" | fora do escopo | nenhum (o sistema deve dizer que não encontrou) |

Os tipos **factual / comparativa / metadados / fora do escopo** serão os *labels* da tabela de roteamento dos agentes (semana 5). O gabarito de cada pergunta será preenchido na semana 6, junto com a avaliação.

## Conjunto de PDFs

Os 9 documentos distintos estão em [`data/pdfs/`](../data/pdfs). A pasta original do grupo tinha 12 arquivos, mas 3 eram cópias (mesmo SHA-256); o pipeline deduplica pelo hash.

| Arquivo | Tipo | Órgão | Páginas | Assunto |
|---|---|---|---|---|
| `NT_2-2025_CGMAN-DINAM-SEB.pdf` | Nota Técnica 2/2025 | MEC/SEB | 4 | Condicionalidade I, exercício 2026 |
| `NT_CJ_3-2026_CGMEM-DINAM-SEB-SEB.pdf` | Nota Técnica 3/2026 | MEC/SEB | 6 | Condicionalidade I, exercício 2027 |
| `NT_4-2025_CGEFE-DAEB-INEP.pdf` | Nota Técnica 4/2025 | INEP | 4 | Condicionalidade II, exercício 2026 |
| `NT_CJ_6-2025_CGEFE-DIRED-INEP.pdf` | Nota Técnica 6/2025 | INEP | 8 | Condicionalidades I e IV, exercício 2026 |
| `NT_CJ_14-2025.pdf` | Nota Técnica Conjunta 14/2025 | INEP | 5 | Condicionalidade V, exercício 2026 |
| `NT_CJ_3-2026.pdf` | Nota Técnica Conjunta 3/2026 | INEP | 8 | Condicionalidade V, exercício 2027 |
| `Resolucao_cif-15_2025.pdf` | Resolução CIF 15/2025 | CIF | 6 | Condicionalidades I, IV e V, exercício 2026 |
| `Resolucao_cif-17_2025.pdf` | Resolução CIF 17/2025 | CIF | 1 | Condicionalidades II e III; indicador VAAT |
| `Portaria_mec_14_2025.pdf` | Portaria Interministerial MEC/MF 14/2025 | MEC/MF | 169 | Edição completa do DOU de 31/12/2025 que contém a portaria |

**Pendente:** o 10º PDF. Sugestão: a própria **Lei nº 14.113/2020** (Lei do Fundeb), citada em todos os outros documentos.

**Observação:** `Portaria_mec_14_2025.pdf` é a edição inteira do DOU, e a maior parte das 169 páginas trata de outros assuntos. Mantivemos o arquivo porque ele é o maior volume do conjunto (e por isso o melhor teste de particionamento no Spark). O ruído que ele traz para a recuperação será medido na semana 6.

## Critérios de sucesso

| Critério | Meta | Medido na |
|---|---|---|
| Todas as páginas dos 10 PDFs extraídas e com metadados | 100% | semana 2 |
| Pipeline Spark executa de ponta a ponta no Databricks | sem erro | semana 3 |
| Documento correto entre os *top-k* trechos recuperados (Recall@5) | ≥ 80% das perguntas | semana 4/6 |
| Respostas corretas segundo o gabarito | ≥ 70% | semana 6 |
| Respostas com citação de documento e página | 100% | semana 5/6 |
| Pergunta fora do escopo respondida com "não encontrei" | 100% | semana 6 |
| Tempo de resposta por pergunta | < 15 s | semana 6 |
