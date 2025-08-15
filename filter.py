#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
from pathlib import Path
from typing import List
import pandas as pd

# Метрики, по которым проверяем, что строка не полностью пустая
METRICS_COLS: List[str] = [
    "main_story_polled", "main_story",
    "main_plus_sides_polled", "main_plus_sides",
    "completionist_polled", "completionist",
    "all_styles_polled", "all_styles",
    "single_player_polled", "single_player",
    "co_op_polled", "co_op",
    "versus_polled", "versus",
]

# Служебные столбцы, которые нужно удалить при фильтрации
SERVICE_COLS: List[str] = ["source_url", "crawled_at"]

# Вспомогательные списки для приведения типов
TIME_COLS = [
    "main_story", "main_plus_sides", "completionist",
    "all_styles", "single_player", "co_op", "versus",
]
POLLED_COLS = [f"{c}_polled" for c in TIME_COLS]
YMD_COLS = ["release_year", "release_month", "release_day"]

# Словари нормализации (пример)
PLATFORM_MAP = {
    "PC": "PC",
    "PC (Windows)": "PC",
    "Playstation 4": "PlayStation 4",
    "PS4": "PlayStation 4",
    "Xbox One": "Xbox One",
    # ...
}
GENRE_MAP = {
    "RTS": "Real Time Strategy",
    "Strategy": "Strategy",
    "Tactics": "Tactics",
    "Action-Adventure": "Action Adventure",
    "RPG": "Role Playing Game",
    # ...
}


def normalize_list_field(val: object, mapping: dict) -> str:
    """Нормализует поле со списком значений через запятую."""
    if pd.isna(val):
        return ""
    parts = [p.strip() for p in str(val).split(",") if p.strip()]
    norm_parts = []
    for p in parts:
        norm_parts.append(mapping.get(p, p))
    return ", ".join(sorted(set(norm_parts)))


def normalize_platforms_series(series: pd.Series) -> pd.Series:
    return series.apply(lambda v: normalize_list_field(v, PLATFORM_MAP))


def normalize_genres_series(series: pd.Series) -> pd.Series:
    return series.apply(lambda v: normalize_list_field(v, GENRE_MAP))


def coerce_dtypes_inplace(df: pd.DataFrame) -> None:
    """Приводим типы, чтобы не было 2015.0 и 90.0."""
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
    for c in YMD_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in POLLED_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in TIME_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def filter_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, int, int, List[str]]:
    """Удаляет строки, где все метрики пустые, и убирает служебные столбцы."""
    missing = [c for c in METRICS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Отсутствуют столбцы: {missing}")
    before = len(df)
    sub = df[METRICS_COLS].copy()
    for c in METRICS_COLS:
        s = sub[c]
        if pd.api.types.is_string_dtype(s) or s.dtype == object:
            sub[c] = s.astype("string").str.strip().replace("", pd.NA)
    mask_all_empty = sub.isna().all(axis=1)
    df = df.loc[~mask_all_empty].copy()
    after = len(df)
    present_to_drop = [c for c in SERVICE_COLS if c in df.columns]
    df.drop(columns=SERVICE_COLS, errors="ignore", inplace=True)
    return df, before, after, present_to_drop


def write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(
        path,
        index=False,
        quoting=csv.QUOTE_ALL,
        lineterminator="\n",
        encoding="utf-8",
        na_rep=""
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Нормализация и фильтрация HLTB датасета.")
    p.add_argument("--src", default="hltb_dataset.csv", help="Путь к исходному CSV.")
    p.add_argument("--out-dir", default=".", help="Папка для результатов.")
    p.add_argument("--chunksize", type=int, default=50000, help="Размер чанка для потоковой обработки.")
    args = p.parse_args()

    src = Path(args.src)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    p_norm = out_dir / "hltb_dataset_normalized.csv"
    p_filt = out_dir / "hltb_dataset_filtered.csv"

    # Шаг 1: потоковая нормализация
    wrote_header = False
    total_rows = 0
    for chunk in pd.read_csv(src, low_memory=False, chunksize=args.chunksize):
        if "platform" in chunk.columns:
            chunk["platform"] = normalize_platforms_series(chunk["platform"])
        if "genres" in chunk.columns:
            chunk["genres"] = normalize_genres_series(chunk["genres"])
        coerce_dtypes_inplace(chunk)
        chunk.to_csv(
            p_norm,
            index=False,
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
            encoding="utf-8",
            na_rep="",
            mode="a",
            header=not wrote_header
        )
        wrote_header = True
        total_rows += len(chunk)

    print(f"[OK] Нормализовано {total_rows} строк → {p_norm.name}")

    # Шаг 2: фильтрация нормализованного файла
    df_norm = pd.read_csv(p_norm, low_memory=False)
    coerce_dtypes_inplace(df_norm)
    df_filt, before, after, dropped_cols = filter_dataframe(df_norm)
    write_csv(df_filt, p_filt)
    print(f"[OK] Фильтрация: удалено {before - after} строк. Осталось {after}.")
    print(f"[OK] Результат → {p_filt.name}")


if __name__ == "__main__":
    main()
