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

# Служебные столбцы, которые нужно удалить
SERVICE_COLS: List[str] = ["source_url", "crawled_at"]


# Нормализация значения столбца type
def normalize_type(x: object) -> str:
    s = "" if pd.isna(x) else str(x).strip().lower()
    s = s.replace("-", " ")
    s = " ".join(s.split())
    if s in {"dlc/expansion", "dlc expansion", "dlc", "expansion"}:
        return "dlc/expansion"
    if s in {"multiplayer focused", "multiplayer"}:
        return "multiplayer focused"
    if s in {"game", "base game", "standalone"}:
        return "game"
    return s


# Фильтрация строк с полностью пустыми метриками
def filter_dataset(df: pd.DataFrame) -> tuple[pd.DataFrame, int, int, List[str]]:
    # Проверяем наличие всех ожидаемых столбцов
    missing = [c for c in METRICS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Отсутствуют ожидаемые столбцы: {missing}")

    before = len(df)

    # Готовим подвыборку для поиска пустых строк
    sub = df[METRICS_COLS].copy()
    for c in METRICS_COLS:
        s = sub[c]
        if pd.api.types.is_string_dtype(s) or s.dtype == object:
            sub[c] = s.astype("string").str.strip().replace("", pd.NA)

    # Маска строк, где все метрики пустые
    mask_all_empty = sub.isna().all(axis=1)

    # Исключаем полностью пустые строки
    df = df.loc[~mask_all_empty].copy()
    after = len(df)

    # Удаляем служебные столбцы, если они есть
    present_to_drop = [c for c in SERVICE_COLS if c in df.columns]
    df.drop(columns=SERVICE_COLS, errors="ignore", inplace=True)

    return df, before, after, present_to_drop


# Разбиение датасета на три выгрузки по типу
def split_exports(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, int]:
    # Проверка наличия обязательного столбца
    if "type" not in df.columns:
        raise ValueError("Отсутствует обязательный столбец 'type' для разбиения по типам.")

    # Создаём нормализованный столбец
    df["_type_norm"] = df["type"].map(normalize_type)

    # Формируем подвыборки
    df_game = df.loc[df["_type_norm"] == "game"].copy()
    df_dlc = df.loc[df["_type_norm"] == "dlc/expansion"].copy()
    df_multi = df.loc[df["_type_norm"] == "multiplayer focused"].copy()

    # Подсчёт прочих строк
    n_other = len(df) - (len(df_game) + len(df_dlc) + len(df_multi))

    # Удаляем служебные поля и столбец нормализации
    for sub in (df_game, df_dlc, df_multi):
        sub.drop(columns=SERVICE_COLS, errors="ignore", inplace=True)
        if "_type_norm" in sub.columns:
            sub.drop(columns=["_type_norm"], inplace=True)

    return df_game, df_dlc, df_multi, n_other


# Запись DataFrame в CSV с кавычками и пустыми значениями
def write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(
        path,
        index=False,
        quoting=csv.QUOTE_ALL,
        lineterminator="\n",
        encoding="utf-8",
        na_rep=""  # Пропуски выводим как пустые строки
    )


def main() -> None:
    # CLI аргументы
    p = argparse.ArgumentParser(description="Фильтрация и разбиение HLTB датасета.")
    p.add_argument("--src", default="hltb_dataset.csv", help="Путь к сырому CSV с заголовком.")
    p.add_argument("--out-dir", default=".", help="Каталог для сохранения результатов.")
    p.add_argument("--filtered-name", dest="filtered_name", default="hltb_dataset_filtered.csv", help="Имя файла после фильтрации.")
    p.add_argument("--out-game", default="hltb_game.csv", help="Имя выгрузки для type=game.")
    p.add_argument("--out-dlc", default="hltb_dlc_expansion.csv", help="Имя выгрузки для type=dlc/expansion.")
    p.add_argument("--out-multi", default="hltb_multiplayer_focused.csv", help="Имя выгрузки для type=multiplayer focused.")
    args = p.parse_args()

    # Подготовка путей
    src = Path(args.src)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    dst_filtered = out_dir / args.filtered_name
    out_game = out_dir / args.out_game
    out_dlc = out_dir / args.out_dlc
    out_multi = out_dir / args.out_multi

    # Шаг 1: Чтение исходного файла
    df_raw = pd.read_csv(src, low_memory=False)

    # Шаг 2: Фильтрация датасета
    df_filtered, before, after, dropped_cols = filter_dataset(df_raw)
    write_csv(df_filtered, dst_filtered)

    # Шаг 3: Разбиение по типам
    df_game, df_dlc, df_multi, n_other = split_exports(df_filtered)

    # Шаг 4: Запись результатов
    write_csv(df_game, out_game)
    write_csv(df_dlc, out_dlc)
    write_csv(df_multi, out_multi)

    # Итоговый вывод
    print(
        f"Done. Removed rows with no time-to-beat values: {before - after}. Remaining rows: {after}.\n"
        f"Removed columns: {dropped_cols}. Output file: {dst_filtered.name}\n"
        f"Within the filtered set of {after} entries: Game: {len(df_game)}. DLC/Expansion: {len(df_dlc)}. Multiplayer focused: {len(df_multi)}. Other/Unrecognized: {n_other}.\n"
    )
    print(
        f"Files:\n"
        f"• {dst_filtered.name}; \n"
        f"• {out_game.name}; \n"
        f"• {out_dlc.name}; \n"
        f"• {out_multi.name}."
    )


if __name__ == "__main__":
    main()
