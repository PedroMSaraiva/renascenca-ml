#!/usr/bin/env python3
"""Baixa dados do Portal de Dados Abertos do TSE, extrai so o *_BRASIL.csv
de cada pacote e confere se as colunas batem com o dicionario de dados
(docs/tse-dicionario.html). O zip e os demais CSVs (por UF) sao apagados
logo depois de extrair, pra nao acumular lixo em disco.

Uso:
    python3 scripts/ingest_tse.py --year 2024
    python3 scripts/ingest_tse.py --year 2024 --datasets consulta_cand bem_candidato

O TSE usa Akamai Bot Manager, que bloqueia por fingerprint de TLS (nao so
User-Agent) — clientes HTTP "puros" (requests, curl padrao) tomam 403 mesmo
com header de navegador. Por isso usamos curl_cffi com impersonate="chrome",
que replica o handshake TLS de um Chrome real.

Depois de extrair, converte o CSV pra parquet em data/processed/ (todas as
colunas como string — os CSVs do TSE usam sentinelas tipo -1/#NULO/#NE
misturadas com numeros, entao cast de tipo fica pra etapa de feature
engineering, nao pra ingestao). Le e grava em chunks pra nao estourar
memoria nos arquivos maiores (prestacao de contas e perfil do eleitorado
passam de 1 GB em CSV).

Requer: pip install curl_cffi pandas pyarrow
"""
import argparse
import csv
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from curl_cffi import requests

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
PROCESSED_DIR = ROOT / "data" / "processed"
REPORT_DIR = ROOT / "data" / "reports"
CHUNKSIZE = 200_000

BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele"

# dataset -> pasta/prefixo no CDN, prefixo do CSV alvo dentro do zip
# (so importa quando o zip empacota mais de um tipo de CSV, como em
# prestacao_de_contas_eleitorais_candidatos) e campos esperados segundo
# o dicionario, pra conferencia.
DATASETS = {
    "consulta_cand": {
        "folder": "consulta_cand",
        "prefix": "consulta_cand",
        "expected": [
            "SQ_CANDIDATO", "NR_CANDIDATO", "NM_CANDIDATO", "DS_CARGO",
            "SG_UF", "SG_UE", "NM_UE", "NR_PARTIDO", "SG_PARTIDO",
            "NM_COLIGACAO", "DS_COMPOSICAO_COLIGACAO", "DS_GENERO",
            "DS_GRAU_INSTRUCAO", "DS_ESTADO_CIVIL", "DS_COR_RACA",
            "DS_OCUPACAO", "DT_NASCIMENTO", "CD_SIT_TOT_TURNO",
            "DS_SIT_TOT_TURNO", "CD_SITUACAO_CANDIDATURA",
            "DS_SITUACAO_CANDIDATURA", "NR_CPF_CANDIDATO",
            "NR_TITULO_ELEITORAL_CANDIDATO", "NR_TURNO",
        ],
    },
    "bem_candidato": {
        "folder": "bem_candidato",
        "prefix": "bem_candidato",
        "expected": [
            "SQ_CANDIDATO", "CD_TIPO_BEM_CANDIDATO", "DS_TIPO_BEM_CANDIDATO",
            "DS_BEM_CANDIDATO", "VR_BEM_CANDIDATO",
        ],
    },
    "votacao_candidato_munzona": {
        "folder": "votacao_candidato_munzona",
        "prefix": "votacao_candidato_munzona",
        "expected": [
            "SQ_CANDIDATO", "NR_TURNO", "SG_UF", "NM_MUNICIPIO", "NR_ZONA",
            "QT_VOTOS_NOMINAIS", "DS_SIT_TOT_TURNO",
        ],
    },
    "prestacao_de_contas_eleitorais_candidatos": {
        # zip nacional pesado (~1.3 GB em 2024): dentro dele cada tipo de
        # registro (receitas, despesas contratadas/pagas, doador originario)
        # vem em um CSV separado, cada um com seu proprio _BRASIL.csv.
        # Ficamos so com o de receitas.
        "folder": "prestacao_contas",
        "prefix": "prestacao_de_contas_eleitorais_candidatos",
        "csv_prefix": "receitas_candidatos",
        "expected": [
            "SQ_CANDIDATO", "DS_ORIGEM_RECEITA", "DS_FONTE_RECEITA",
            "VR_RECEITA", "DS_CARGO", "DS_ESPECIE_RECEITA",
        ],
    },
    "perfil_eleitorado": {
        "folder": "perfil_eleitorado",
        "prefix": "perfil_eleitorado",
        "expected": [
            "SG_UF", "NM_MUNICIPIO", "NR_ZONA", "DS_FAIXA_ETARIA",
            "DS_GENERO", "DS_GRAU_ESCOLARIDADE", "QT_ELEITORES",
        ],
    },
}


def download_zip(dataset: str, year: int, dest_dir: Path) -> Path:
    cfg = DATASETS[dataset]
    url = f"{BASE_URL}/{cfg['folder']}/{cfg['prefix']}_{year}.zip"
    zip_path = dest_dir / f"{cfg['prefix']}_{year}.zip"

    print(f"[{dataset}] baixando {url}")
    resp = requests.get(url, impersonate="chrome", timeout=180)
    resp.raise_for_status()
    zip_path.write_bytes(resp.content)
    print(f"[{dataset}] baixado ({len(resp.content) / 1e6:.1f} MB)")
    return zip_path


def find_brasil_csv(zip_path: Path, year: int, csv_prefix: str) -> str:
    """Localiza o CSV consolidado nacional (*_BRASIL.csv) dentro do zip.
    Quando o zip empacota mais de um tipo de registro, csv_prefix escolhe
    qual (ex.: "receitas_candidatos" em vez de "despesas_pagas_..." ou
    "receitas_candidatos_doador_originario"). Casa o nome exato, nao so o
    prefixo — "receitas_candidatos" e prefixo de
    "receitas_candidatos_doador_originario" tambem, entao startswith sozinho
    pega o arquivo errado."""
    with zipfile.ZipFile(zip_path) as zf:
        names = zf.namelist()
    exact = f"{csv_prefix}_{year}_BRASIL.csv".upper()
    exact_matches = [n for n in names if n.upper() == exact]
    if exact_matches:
        return exact_matches[0]
    brasil_matches = [n for n in names if n.upper().endswith("_BRASIL.CSV")]
    if not brasil_matches:
        raise FileNotFoundError(f"Nenhum *_BRASIL.csv encontrado em {zip_path}")
    return brasil_matches[0]


def extract_only(zip_path: Path, member: str, dest_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as zf:
        zf.extract(member, dest_dir)
    extracted = dest_dir / member
    # normaliza pra ficar direto em dest_dir, sem sub-hierarquia do zip
    final_path = dest_dir / Path(member).name
    if extracted != final_path:
        extracted.rename(final_path)
        # limpa pasta intermediaria vazia deixada pelo extract, se houver
        parent = extracted.parent
        while parent != dest_dir and parent.exists() and not any(parent.iterdir()):
            parent.rmdir()
            parent = parent.parent
    return final_path


def read_header_and_sample(csv_path: Path, n_rows: int = 5):
    with csv_path.open(encoding="latin-1", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        sample = [next(reader, None) for _ in range(n_rows)]
        sample = [row for row in sample if row is not None]
    return header, sample


def csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    try:
        chunks = pd.read_csv(
            csv_path, sep=";", encoding="latin-1", dtype=str, chunksize=CHUNKSIZE,
        )
        for chunk in chunks:
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema, compression="snappy")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()


def compare_columns(dataset: str, header: list[str]) -> dict:
    expected = set(DATASETS[dataset]["expected"])
    found = set(c.strip().upper() for c in header)
    return {
        "matched": sorted(expected & found),
        "missing_from_file": sorted(expected - found),
        "extra_in_file": sorted(found - expected),
    }


def write_report(dataset: str, year: int, csv_path: Path, header, sample, diff):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"{dataset}_{year}_BRASIL.md"
    lines = [
        f"# {dataset} — {year} / BRASIL",
        "",
        f"Arquivo: {csv_path.name} ({csv_path.stat().st_size / 1e6:.1f} MB)",
        f"Colunas no arquivo real: {len(header)}",
        "",
        "## Confronto com o dicionario",
        f"- Bateram: {', '.join(diff['matched']) or '(nenhuma)'}",
        f"- No dicionario mas ausentes no arquivo: {', '.join(diff['missing_from_file']) or '(nenhuma)'}",
        f"- No arquivo mas fora do dicionario: {', '.join(diff['extra_in_file']) or '(nenhuma)'}",
        "",
        "## Header completo",
        "```",
        "; ".join(header),
        "```",
        "",
        "## Amostra (5 linhas)",
        "```",
    ]
    for row in sample:
        lines.append("; ".join(row))
    lines.append("```")
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[{dataset}] relatorio em {path}")


def run(dataset: str, year: int) -> bool:
    cfg = DATASETS[dataset]
    dest_dir = RAW_DIR / dataset / str(year)
    dest_dir.mkdir(parents=True, exist_ok=True)

    expected_name = f"{cfg.get('csv_prefix', cfg['prefix'])}_{year}_BRASIL.csv"
    csv_path = dest_dir / expected_name
    if csv_path.exists():
        print(f"[{dataset}] {csv_path.name} ja existe, pulando download/extracao")
    else:
        zip_path = download_zip(dataset, year, dest_dir)
        try:
            member = find_brasil_csv(zip_path, year, cfg.get("csv_prefix", cfg["prefix"]))
            csv_path = extract_only(zip_path, member, dest_dir)
            print(f"[{dataset}] extraido {csv_path.name} "
                  f"({csv_path.stat().st_size / 1e6:.1f} MB)")
        finally:
            zip_path.unlink(missing_ok=True)
            print(f"[{dataset}] zip apagado")

    header, sample = read_header_and_sample(csv_path)
    diff = compare_columns(dataset, header)
    write_report(dataset, year, csv_path, header, sample, diff)

    parquet_path = PROCESSED_DIR / dataset / str(year) / (csv_path.stem + ".parquet")
    if parquet_path.exists():
        print(f"[{dataset}] {parquet_path.relative_to(ROOT)} ja existe, pulando conversao")
    else:
        csv_to_parquet(csv_path, parquet_path)
    csv_mb = csv_path.stat().st_size / 1e6
    parquet_mb = parquet_path.stat().st_size / 1e6
    print(f"[{dataset}] parquet {parquet_path.relative_to(ROOT)} "
          f"({parquet_mb:.1f} MB, {parquet_mb / csv_mb:.0%} do CSV)")

    ok = not diff["missing_from_file"]
    status = "OK" if ok else "DIVERGENTE"
    print(f"[{dataset}] status: {status} "
          f"(faltando: {len(diff['missing_from_file'])}, extras: {len(diff['extra_in_file'])})")
    return ok


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--datasets", nargs="+", choices=list(DATASETS),
                         default=list(DATASETS))
    args = parser.parse_args()

    results = {}
    for dataset in args.datasets:
        try:
            results[dataset] = run(dataset, args.year)
        except Exception as exc:  # noqa: BLE001 - relatar e seguir para os proximos datasets
            print(f"[{dataset}] ERRO: {exc}", file=sys.stderr)
            results[dataset] = None

    print("\n=== Resumo ===")
    for dataset, ok in results.items():
        label = "OK" if ok else ("ERRO" if ok is None else "DIVERGENTE")
        print(f"{dataset}: {label}")


if __name__ == "__main__":
    main()
