#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import contextlib
import csv
import datetime as dt
import json
import os
import random
import re
import sys
import traceback
from typing import Any, Dict, Optional, Tuple, List

import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup

DEFAULT_CSV_PATH = "hltb_dataset.csv"
DEFAULT_LOG_PATH = "hltb.log"

CSV_HEADERS = [
    "id", "name", "type",
    "release_date", "release_precision", "release_year", "release_month", "release_day",
    "main_story_polled", "main_story",
    "main_plus_sides_polled", "main_plus_sides",
    "completionist_polled", "completionist",
    "all_styles_polled", "all_styles",
    "single_player_polled", "single_player",
    "co_op_polled", "co_op",
    "versus_polled", "versus",
    "source_url", "crawled_at"
]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}

TIME_KEYS = [
    "main_story", "main_plus_sides", "completionist", "all_styles",
    "single_player", "co_op", "versus"
]
POLLED_KEYS = [f"{k}_polled" for k in TIME_KEYS]


# ----------------------------- Логирование в консоль и файл -----------------------------

def log(msg: str, file) -> None:
    print(msg)
    file.write(msg + "\n")
    file.flush()


# ----------------------------- Утилиты нормализации -----------------------------

def norm_time_label(label: str) -> Optional[str]:
    lbl = label.strip().lower()
    mapping = {
        "main story": "main_story",
        "main + sides": "main_plus_sides",
        "main + extras": "main_plus_sides",
        "completionist": "completionist",
        "all styles": "all_styles",
        "all playstyles": "all_styles",
        "single-player": "single_player",
        "single player": "single_player",
        "singleplayer": "single_player",
        "co-op": "co_op",
        "coop": "co_op",
        "competitive": "versus",
        "vs.": "versus",
        "versus": "versus",
    }
    return mapping.get(lbl)


def parse_hours(text: str) -> Optional[float]:
    if not text:
        return None
    raw = text.replace("\xa0", " ").strip().lower()
    if raw in {"--", "-"}:
        return None

    if "-" in raw or "–" in raw or "—" in raw:
        parts = re.split(r"\s*[-–—]\s*", raw)
        if len(parts) == 2:
            a = parse_hours(parts[0]); b = parse_hours(parts[1])
            if a is not None and b is not None:
                avg = round((a + b) / 2.0, 2)
                return int(avg) if float(avg).is_integer() else avg

    m = re.match(r"^(\d+)\s*½\s*h(?:our)?s?\b", raw, flags=re.I)
    if m:
        val = float(m.group(1)) + 0.5
        return int(val) if float(val).is_integer() else val

    if re.match(r"^½\s*h(?:our)?s?\b", raw, flags=re.I):
        return 0.5

    m = re.match(r"^(\d+)\s*h\s*(\d+)\s*m\b", raw, flags=re.I)
    if m:
        hours = int(m.group(1)); minutes = int(m.group(2))
        val = round(hours + minutes / 60.0, 2)
        return int(val) if float(val).is_integer() else val

    m = re.match(r"^(\d+)\s*h\b", raw, flags=re.I)
    if m:
        return int(m.group(1))

    m = re.match(r"^(\d+)\s*m\b", raw, flags=re.I)
    if m:
        val = round(int(m.group(1)) / 60.0, 2)
        return int(val) if float(val).is_integer() else val

    m = re.match(r"^(\d+)\s*(mins?|minutes?)\b", raw, flags=re.I)
    if m:
        val = round(int(m.group(1)) / 60.0, 2)
        return int(val) if float(val).is_integer() else val

    m = re.match(r"^(\d+)\s*h(?:our)?s?\b", raw, flags=re.I)
    if m:
        return int(m.group(1))

    return None


def to_int(text: str) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"\d[\d,]*", text)
    if not m:
        return None
    try:
        return int(m.group(0).replace(",", ""))
    except ValueError:
        return None


def ensure_time_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    for k in TIME_KEYS + POLLED_KEYS:
        d.setdefault(k, None)
    return d


# ----------------------------- Парсинг HTML -----------------------------

def extract_id_from_url(url: str) -> int:
    m = re.search(r"/game/(\d+)$", url)
    if not m:
        raise ValueError("Не удалось извлечь ID игры из URL.")
    return int(m.group(1))


def parse_name_from_page(soup: BeautifulSoup) -> Optional[str]:
    div = soup.find("div", class_=re.compile(r"GameHeader_profile_header__.*"))
    if div:
        text = div.get_text(strip=True)
        if text:
            return text
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        if not tag.string:
            continue
        try:
            data = json.loads(tag.string)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("name"), str):
                return it["name"]
    return None


def parse_times_from_tables(soup: BeautifulSoup) -> Dict[str, Optional[float]]:
    result: Dict[str, Optional[float]] = {k: None for k in TIME_KEYS + POLLED_KEYS}
    tables = soup.find_all("table", class_=re.compile(r"GameTimeTable_game_main_table__"))
    if not tables:
        return result

    for table in tables:
        thead = table.find("thead")
        section = None
        if thead:
            first_td = thead.find("td")
            if first_td:
                section = first_td.get_text(" ", strip=True).strip().lower()

        tbody = table.find("tbody")
        if not tbody:
            continue

        for tr in tbody.find_all("tr", class_=re.compile(r"spreadsheet")):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue
            label = tds[0].get_text(" ", strip=True)
            polled_text = tds[1].get_text(" ", strip=True)
            avg_text = tds[2].get_text(" ", strip=True)

            key = norm_time_label(label)
            if key is None:
                continue

            avg_val = None if not avg_text or avg_text.strip().lower() in {"--", "-"} else parse_hours(avg_text)
            polled_val = to_int(polled_text)

            if section == "single-player" and key == "main_story":
                if avg_val is not None:
                    result["single_player"] = avg_val
                if polled_val is not None:
                    result["single_player_polled"] = polled_val

            if avg_val is not None:
                result[key] = avg_val
            if polled_val is not None:
                result[f"{key}_polled"] = polled_val

    return result


def parse_times_from_page(soup: BeautifulSoup) -> Dict[str, Optional[float]]:
    result: Dict[str, Optional[float]] = {k: None for k in TIME_KEYS}
    stats = soup.find("div", class_=re.compile(r"GameStats_game_times__.*"))
    if not stats:
        return result

    extra: Dict[str, Optional[float]] = {}
    for li in stats.find_all("li"):
        h4 = li.find("h4"); h5 = li.find("h5")
        if not h4 or not h5:
            continue
        label = h4.get_text(" ", strip=True)
        value_text = h5.get_text(" ", strip=True)
        key = norm_time_label(label)
        if not key:
            continue
        val = parse_hours(value_text)
        if key in {"single_player", "co_op", "versus"}:
            extra[key] = val
        else:
            result[key] = val

    if result.get("main_story") is None and extra.get("single_player") is not None:
        result["main_story"] = extra["single_player"]

    result.update(extra)
    return ensure_time_keys(result)


def detect_content_type(soup: BeautifulSoup) -> str:
    flags: List[str] = []
    for div in soup.find_all("div", class_=re.compile(r"GameSummary_profile_info__.*")):
        text = " ".join(div.stripped_strings).lower()
        if "note:" in text:
            if "dlc/expansion" in text:
                flags.append("dlc/expansion")
            if "multiplayer focused" in text:
                flags.append("multiplayer focused")
    return "game" if not flags else "; ".join(sorted(set(flags)))


def parse_release_info(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    texts = []
    for div in soup.find_all("div", class_=re.compile(r"GameSummary_profile_info__.*")):
        txt = " ".join(div.stripped_strings)
        if txt:
            texts.append(txt)

    for text in texts:
        m = re.search(r"[A-Z]{2,3}:\s*([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})", text, re.I)
        if m:
            month_name = m.group(1).lower()
            day = int(m.group(2)); year = int(m.group(3))
            month = MONTHS.get(month_name)
            if month:
                return {
                    "release_date": f"{year:04d}-{month:02d}-{day:02d}",
                    "release_precision": "day",
                    "release_year": f"{year:04d}",
                    "release_month": f"{month:02d}",
                    "release_day": f"{day:02d}",
                }

    for text in texts:
        m = re.search(r"[A-Z]{2,3}:\s*([A-Za-z]+)\s+(\d{4})\b", text, re.I)
        if m:
            month_name = m.group(1).lower(); year = int(m.group(2))
            month = MONTHS.get(month_name)
            if month:
                return {
                    "release_date": f"{year:04d}-{month:02d}",
                    "release_precision": "month",
                    "release_year": f"{year:04d}",
                    "release_month": f"{month:02d}",
                    "release_day": None,
                }

    for text in texts:
        m = re.search(r"[A-Z]{2,3}:\s*(\d{4})\b", text)
        if m:
            year = int(m.group(1))
            return {
                "release_date": f"{year:04d}",
                "release_precision": "year",
                "release_year": f"{year:04d}",
                "release_month": None,
                "release_day": None,
            }

    return {
        "release_date": None,
        "release_precision": None,
        "release_year": None,
        "release_month": None,
        "release_day": None,
    }


def parse_release_date_legacy(soup: BeautifulSoup) -> Optional[str]:
    for div in soup.find_all("div", class_=re.compile(r"GameSummary_profile_info__.*")):
        text = " ".join(div.stripped_strings)
        m = re.search(r"([A-Z]{2,3}):\s*([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})", text, re.I)
        if m:
            month_name = m.group(2).lower(); day = int(m.group(3)); year = int(m.group(4))
            month = MONTHS.get(month_name)
            if month:
                return f"{year:04d}-{month:02d}-{day:02d}"
    return None


def merge_release_info(primary: Dict[str, Optional[str]], fallback_date: Optional[str]) -> Dict[str, Optional[str]]:
    out = dict(primary)
    if not out.get("release_date") and fallback_date:
        y, m, d = fallback_date.split("-")
        out.update({
            "release_date": fallback_date,
            "release_precision": "day",
            "release_year": y,
            "release_month": m,
            "release_day": d,
        })
    return out


def parse_hltb_game_from_html(url: str, html: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    name = parse_name_from_page(soup)
    if not name:
        return None

    content_type = detect_content_type(soup)

    times = parse_times_from_tables(soup)
    if all(times.get(k) is None for k in TIME_KEYS):
        times = parse_times_from_page(soup)
    times = ensure_time_keys(times)

    ri = parse_release_info(soup)
    legacy = parse_release_date_legacy(soup)
    ri = merge_release_info(ri, legacy)

    game_id = extract_id_from_url(url)
    now_iso = dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    record: Dict[str, Any] = {
        "id": str(game_id),
        "name": name,
        "type": content_type,
        **ri,
        **times,
        "source_url": url,
        "crawled_at": now_iso
    }
    return record


# ----------------------------- Сетевой слой (async) -----------------------------

class Fetcher:
    def __init__(self, session: ClientSession, log_file, concurrency: int, base_delay: float = 0.25, jitter: float = 0.35):
        self.session = session
        self.semaphore = asyncio.Semaphore(concurrency)
        self.base_delay = base_delay
        self.jitter = jitter
        self.log = log_file

    async def polite_sleep(self):
        await asyncio.sleep(self.base_delay + random.random() * self.jitter)

    async def fetch_html(self, url: str, max_attempts: int = 5) -> Optional[str]:
        attempt = 0
        backoff = 0.6
        while attempt < max_attempts:
            attempt += 1
            async with self.semaphore:
                try:
                    async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status == 404:
                            return None
                        if resp.status in {200}:
                            text = await resp.text()
                            if text:
                                return text
                        if resp.status in {429, 500, 502, 503, 504}:
                            await self._warn(url, f"retryable status {resp.status}, attempt {attempt}")
                        else:
                            await self._warn(url, f"bad status {resp.status}, attempt {attempt}")
                except asyncio.TimeoutError:
                    await self._warn(url, f"timeout, attempt {attempt}")
                except aiohttp.ClientError as e:
                    await self._warn(url, f"client_error={repr(e)}, attempt {attempt}")

            await asyncio.sleep(backoff + random.random() * 0.4)
            backoff *= 1.7

        return None

    async def _warn(self, url: str, msg: str):
        log(f"[WARN] {url} — {msg}", self.log)


# ----------------------------- Пайплайн: producer/consumer -----------------------------

async def producer(fetcher: "Fetcher",
                   start_id: int,
                   out_q: "asyncio.Queue[Tuple[int, Optional[Dict[str, Any]], Optional[str]]]",
                   stop_event: asyncio.Event,
                   end_id: Optional[int] = None):
    i = start_id
    while not stop_event.is_set():
        if end_id is not None and i >= end_id:
            break
        url = f"https://howlongtobeat.com/game/{i}"
        html = await fetcher.fetch_html(url)
        if html is None:
            await out_q.put((i, None, None))
            await fetcher.polite_sleep()
            i += 1
            continue
        try:
            data = parse_hltb_game_from_html(url, html)
        except Exception as e:
            err = f"{repr(e)}\n{traceback.format_exc()}"
            await out_q.put((i, None, err))
            await fetcher.polite_sleep()
            i += 1
            continue

        await out_q.put((i, data, None))
        await fetcher.polite_sleep()
        i += 1


async def consumer(out_q: "asyncio.Queue[Tuple[int, Optional[Dict[str, Any]], Optional[str]]]",
                   writer: csv.DictWriter,
                   log_file,
                   existing_ids: set,
                   stop_event: asyncio.Event,
                   miss_threshold: int):
    processed = 0
    consecutive_skips = 0
    while True:
        item = await out_q.get()
        if item is None:
            out_q.task_done()
            break

        game_id, data, err = item

        if err:
            log(f"[ERROR] ID {game_id} — {err}", log_file)
            out_q.task_done()
            continue

        if data is None:
            consecutive_skips += 1
            log(f"[SKIP]  ID {game_id} — нет данных или 404 (streak={consecutive_skips}/{miss_threshold})", log_file)
            if consecutive_skips >= miss_threshold and not stop_event.is_set():
                log(f"[STOP]  Достигнут порог подряд: {miss_threshold} пропусков. Останов.", log_file)
                stop_event.set()
            out_q.task_done()
            continue

        consecutive_skips = 0

        if data["id"] in existing_ids:
            log(f"[DUP]   ID {game_id} — пропущен (уже есть в CSV)", log_file)
            out_q.task_done()
            continue

        writer.writerow(data)
        existing_ids.add(data["id"])
        log(f"[OK]    ID {game_id} — {data.get('name')}", log_file)

        processed += 1
        if processed % 100 == 0:
            log_file.flush()
        out_q.task_done()


# ----------------------------- Вспомогательные функции -----------------------------

def read_existing_ids(csv_path: str) -> set:
    ids = set()
    if not os.path.exists(csv_path):
        return ids
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid = row.get("id")
            if rid:
                ids.add(rid)
    return ids


def get_resume_start(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 1
    last_id = 0
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                last_id = max(last_id, int(row["id"]))
            except Exception:
                continue
    return last_id + 1


# ----------------------------- Точка входа -----------------------------

async def main_async(args):
    csv_path = args.csv
    log_path = args.log
    concurrency = max(1, args.concurrency)

    file_exists = os.path.exists(csv_path)
    existing_ids = read_existing_ids(csv_path)

    if args.start is not None and args.start > 0:
        start_id = args.start
    else:
        start_id = get_resume_start(csv_path)

    infinite = isinstance(args.count, str) and args.count.strip() == "*"
    end_id = None if infinite else start_id + max(0, int(args.count))

    connector = aiohttp.TCPConnector(limit=concurrency * 4, ttl_dns_cache=300)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }

    out_q: asyncio.Queue = asyncio.Queue(maxsize=concurrency * 8)
    stop_event = asyncio.Event()
    miss_threshold = int(args.miss_threshold)

    with open(csv_path, "a", newline="", encoding="utf-8-sig") as f_csv, \
         open(log_path, "a", encoding="utf-8") as f_log:

        writer = csv.DictWriter(
            f_csv,
            fieldnames=CSV_HEADERS,
            quoting=csv.QUOTE_ALL,
            escapechar='\\'
        )
        if not file_exists:
            writer.writeheader()

        mode = "*" if infinite else f"{int(args.count)}"
        log(f"[RESUME] start_id={start_id} mode={mode} concurrency={concurrency} miss_threshold={miss_threshold}", f_log)

        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
            fetcher = Fetcher(session=session, log_file=f_log, concurrency=concurrency)

            prod_task = asyncio.create_task(producer(fetcher, start_id, out_q, stop_event, end_id=end_id))
            cons_task = asyncio.create_task(consumer(out_q, writer, f_log, existing_ids, stop_event, miss_threshold))

            try:
                await prod_task
                await out_q.put(None)
                await cons_task
            except KeyboardInterrupt:
                log("[ABORT] Получен KeyboardInterrupt, корректное завершение…", f_log)
                stop_event.set()
                await out_q.put(None)
            finally:
                if not prod_task.done():
                    prod_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await prod_task
                f_log.flush()


def main():
    parser = argparse.ArgumentParser(prog="hltb-parser-async", description="HowLongToBeat parser (async)")
    parser.add_argument("count", nargs="?", default="1000",
                        help='сколько ID обработать подряд. Число или "*" для режима без верхней границы с автостопом')
    parser.add_argument("--start", type=int, default=None, help="стартовый ID; если не задан, берётся из CSV")
    parser.add_argument("--concurrency", type=int, default=8, help="число одновременных запросов; дефолт 8")
    parser.add_argument("--csv", type=str, default=DEFAULT_CSV_PATH, help=f"путь к CSV; дефолт {DEFAULT_CSV_PATH}")
    parser.add_argument("--log", type=str, default=DEFAULT_LOG_PATH, help=f"путь к логу; дефолт {DEFAULT_LOG_PATH}")
    parser.add_argument("--miss-threshold", type=int, default=400,
                        help="порог подряд для 'нет данных/404' в режиме '*'; дефолт 400")
    args = parser.parse_args()

    try:
        asyncio.run(main_async(args))
    except Exception as e:
        sys.stderr.write(f"Fatal: {repr(e)}\n")
        sys.stderr.write(traceback.format_exc() + "\n")
        sys.exit(1)


if __name__ == "__main__":
    main()