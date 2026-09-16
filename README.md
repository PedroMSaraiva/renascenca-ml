# renascenca

Pipeline de ingestão e análise de dados eleitorais do TSE (Tribunal Superior
Eleitoral), a partir do [Portal de Dados Abertos](https://dadosabertos.tse.jus.br/).

Baixa os pacotes públicos do TSE, extrai o CSV consolidado nacional
(`*_BRASIL.csv`) de cada um, confere as colunas contra o dicionário de dados
oficial (`docs/tse-dicionario.html`) e converte tudo para Parquet em
`data/processed/`, pronto para análise em `notebooks/`.

## Datasets

| Dataset | Conteúdo |
|---|---|
| `consulta_cand` | Cadastro de candidatos (nome, partido, cargo, situação) |
| `bem_candidato` | Bens declarados por candidato |
| `votacao_candidato_munzona` | Votos por candidato/município/zona |
| `prestacao_de_contas_eleitorais_candidatos` | Receitas de campanha |
| `perfil_eleitorado` | Perfil do eleitorado por município/zona |

## Estrutura

```
data/
  raw/         CSVs baixados do TSE (gitignored, ~9GB)
  processed/   Parquet convertido (gitignored, ~600MB)
  reports/     Conferência de colunas por dataset/ano (versionado)
docs/
  tse-dicionario.html   Dicionário de dados oficial do TSE
notebooks/
  eda_tse.ipynb         Análise exploratória
scripts/
  ingest_tse.py          Download + conversão para parquet
```

`data/raw/` e `data/processed/` não vão para o git — são regenerados
rodando o script de ingestão.

## Quickstart

Requer Python 3.13+ e [uv](https://docs.astral.sh/uv/).

```bash
git clone git@github.com:PedroMSaraiva/renascenca-ml.git
cd renascenca-ml
uv sync
```

Baixar e processar um ano (todos os datasets):

```bash
uv run scripts/ingest_tse.py --year 2024
```

Ou só alguns datasets:

```bash
uv run scripts/ingest_tse.py --year 2024 --datasets consulta_cand bem_candidato
```

Isso baixa o zip de cada dataset, extrai o CSV nacional, apaga o zip e os
CSVs por UF (não acumula lixo em disco), grava um relatório de conferência
em `data/reports/` e converte para `data/processed/<dataset>/<ano>/*.parquet`.
Já executado antes? Ele pula download e conversão se o arquivo já existir.

Depois, abrir a análise exploratória:

```bash
uv run jupyter notebook notebooks/eda_tse.ipynb
```

## Notas

- O TSE usa Akamai Bot Manager, que bloqueia por fingerprint de TLS — por
  isso o download usa `curl_cffi` com `impersonate="chrome"` em vez de
  `requests` puro.
- Todas as colunas são lidas como string na ingestão: os CSVs do TSE
  misturam sentinelas (`-1`, `#NULO`, `#NE`) com valores numéricos, então
  o cast de tipo fica para a etapa de feature engineering.
- Arquivos grandes (prestação de contas, perfil do eleitorado passam de
  1GB em CSV) são lidos e gravados em chunks para não estourar memória.
