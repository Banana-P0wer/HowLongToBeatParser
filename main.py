import re
import json
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any
import csv
import traceback
import os
from time import sleep
import argparse

CSV_PATH = "hltb_dataset.csv"
LOG_PATH = "hltb_errors.log"

CSV_HEADERS = [
    "id", "name",
    "release_date", "release_precision", "release_year", "release_month", "release_day",
    "main_story", "main_plus_sides", "completionist", "all_styles",
    "single_player", "co_op", "versus",
]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

TIME_LABELS = {
    "Main Story": "main_story",
    "Main + Sides": "main_plus_sides",
    "Completionist": "completionist",
    "All Styles": "all_styles",
}


def extract_id_from_url(url: str) -> int:
    m = re.search(r"/game/(\d+)", url)
    if not m:
        raise ValueError("Не удалось извлечь ID игры из URL.")
    return int(m.group(1))


def normalize_time_text(s: str) -> str:
    return " ".join(s.replace("\xa0", " ").strip().split())


def fetch_html(url: str) -> Optional[str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=20)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    return resp.text


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
        if isinstance(data, dict) and isinstance(data.get("name"), str):
            return data["name"]
        if isinstance(data, list):
            for it in data:
                if isinstance(it, dict) and isinstance(it.get("name"), str):
                    return it["name"]
    return None


def parse_hours(text: str) -> Optional[float]:
    text = text.strip().lower().replace("\xa0", " ")

    m = re.match(r"^(\d+)\s*½\s*hour", text)
    if m:
        return float(m.group(1)) + 0.5

    if re.match(r"^½\s*hour", text):
        return 0.5

    m = re.match(r"^(\d+)\s*(mins?|minutes?)\b", text)
    if m:
        value = round(int(m.group(1)) / 60.0, 1)
        return int(value) if value.is_integer() else value

    m = re.match(r"^(\d+)\s*hours?", text)
    if m:
        return int(m.group(1))

    m = re.match(r"^(\d+)\s*h\s*(\d+)\s*m", text)
    if m:
        hours = int(m.group(1))
        minutes = int(m.group(2))
        value = round(hours + minutes / 60.0, 1)
        return int(value) if value.is_integer() else value

    return None


def parse_times_from_page(soup: BeautifulSoup) -> Dict[str, Optional[float]]:
    result = {v: None for v in TIME_LABELS.values()}

    stats = soup.find("div", class_=re.compile(r"GameStats_game_times__.*"))
    if not stats:
        return result

    label_map_core = {
        "main story": "main_story",
        "main + sides": "main_plus_sides",
        "completionist": "completionist",
        "all styles": "all_styles",
    }
    label_map_extra = {
        "single-player": "single_player",
        "single player": "single_player",
        "singleplayer": "single_player",
        "co-op": "co_op",
        "coop": "co_op",
        "vs.": "versus",
        "versus": "versus",
    }

    extra: Dict[str, Optional[float]] = {}

    for li in stats.find_all("li"):
        h4 = li.find("h4")
        h5 = li.find("h5")
        if not h4 or not h5:
            continue

        label = h4.get_text(" ", strip=True).strip().lower()
        value_text = h5.get_text(" ", strip=True)  # только значение в рамках текущего li

        if label in label_map_core:
            result[label_map_core[label]] = parse_hours(value_text)
        elif label in label_map_extra:
            extra[label_map_extra[label]] = parse_hours(value_text)

    if result.get("main_story") is None and extra.get("single_player") is not None:
        result["main_story"] = extra["single_player"]

    # если ты ведёшь отдельные столбцы для сетевых режимов — это сохранит их
    result.update(extra)

    return result


def parse_release_date(soup: BeautifulSoup) -> Optional[str]:
    divs = soup.find_all("div", class_=re.compile(r"GameSummary_profile_info__.*"))
    for div in divs:
        text = " ".join(div.stripped_strings)

        # Дата с шаблоном "<2-3 буквы>: Month 17th, 2025"
        m = re.search(r"([A-Z]{2,3}):\s*([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})", text, re.I)
        if m:
            month_name = m.group(2).lower()
            day = int(m.group(3))
            year = int(m.group(4))

            MONTHS = {
                "january": 1, "february": 2, "march": 3, "april": 4,
                "may": 5, "june": 6, "july": 7, "august": 8,
                "september": 9, "october": 10, "november": 11, "december": 12
            }
            month = MONTHS.get(month_name)
            if month:
                return f"{year:04d}-{month:02d}-{day:02d}"

    return None


def parse_release_info(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    MONTHS = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }

    texts = []
    for div in soup.find_all("div", class_=re.compile(r"GameSummary_profile_info__.*")):
        txt = " ".join(div.stripped_strings)
        if txt:
            texts.append(txt)

    for text in texts:
        # 1) День, месяц, год: "<CC>: Month 8th, 2018"
        m = re.search(r"[A-Z]{2,3}:\s*([A-Za-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})", text, re.I)
        if m:
            month_name = m.group(1).lower()
            day = int(m.group(2))
            year = int(m.group(3))
            month = MONTHS.get(month_name)
            if month:
                return {
                    "release_date": f"{year:04d}-{month:02d}-{day:02d}",
                    "release_precision": "day",
                    "release_year": f"{year:04d}",
                    "release_month": f"{month:02d}",
                    "release_day": f"{day:02d}",
                }

        # 2) Месяц и год: "<CC>: Month 2019"
        m = re.search(r"[A-Z]{2,3}:\s*([A-Za-z]+)\s+(\d{4})\b", text, re.I)
        if m:
            month_name = m.group(1).lower()
            year = int(m.group(2))
            month = MONTHS.get(month_name)
            if month:
                return {
                    "release_date": f"{year:04d}-{month:02d}",
                    "release_precision": "month",
                    "release_year": f"{year:04d}",
                    "release_month": f"{month:02d}",
                    "release_day": None,
                }

        # 3) Только год: "<CC>: 2012"
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


def parse_skip_reason(soup: BeautifulSoup) -> Optional[str]:
    # Ищем блоки примечаний и проверяем наличие ключевых меток
    for div in soup.find_all("div", class_=re.compile(r"GameSummary_profile_info__.*")):
        text = " ".join(div.stripped_strings).lower()
        if "note:" in text:
            if "dlc/expansion" in text:
                return "DLC/Expansion"
            if "multiplayer focused" in text:
                return "Multiplayer Focused"
    return None


def parse_hltb_game(url: str) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    html = fetch_html(url)
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")

    name = parse_name_from_page(soup)

    skip_reason = parse_skip_reason(soup)
    if skip_reason:
        pretty_reason = f"{name} — {skip_reason}" if name else skip_reason
        return None, pretty_reason

    if not name:
        return None, None

    times = parse_times_from_page(soup)
    release_date = parse_release_date(soup)
    release_info = parse_release_info(soup)  # Новый парсер, возвращает дополнительные поля

    try:
        game_id = extract_id_from_url(url)
    except ValueError:
        return None, None

    return ({
        "id": str(game_id),
        "name": name,
        "release_date": release_date,
        **release_info,  # включает release_precision, release_year, release_month, release_day
        **times
    }, None)


def get_last_processed_id() -> int:
    if not os.path.exists(CSV_PATH):
        return 0
    last_id = 0
    with open(CSV_PATH, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                last_id = max(last_id, int(row["id"]))
            except (KeyError, ValueError):
                continue
    return last_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="hltb-parser", description="HowLongToBeat CSV parser")
    parser.add_argument("count", nargs="?", type=int, default=1000,
                        help="сколько ID обработать подряд, по умолчанию 1000")
    parser.add_argument("--start", type=int, default=None,
                        help="необязательный стартовый ID; если не задан, берётся из CSV")
    args = parser.parse_args()

    # определяем старт
    if args.start is not None and args.start > 0:
        start_id = args.start
    else:
        start_id = get_last_processed_id() + 1

    # определяем финиш по аргументу count
    end_id = start_id + max(0, args.count)

    file_exists = os.path.exists(CSV_PATH)

    with open(CSV_PATH, "a", newline="", encoding="utf-8-sig") as f_csv, \
         open(LOG_PATH, "a", encoding="utf-8") as f_log:

        writer = csv.DictWriter(
            f_csv,
            fieldnames=CSV_HEADERS,
            quoting=csv.QUOTE_ALL,
            escapechar='\\'
        )
        if not file_exists:
            writer.writeheader()

        f_log.write(f"[RESUME] start_id={start_id} count={args.count} end_id={end_id-1}\n")

        for i in range(start_id, end_id):
            url = f"https://howlongtobeat.com/game/{i}"
            try:
                data, skip_reason = parse_hltb_game(url)

                if skip_reason:
                    f_log.write(f"[SKIP-NOTE] ID {i} — {skip_reason}\n")
                    continue

                if data is None:
                    f_log.write(f"[SKIP] ID {i} — нет данных или 404\n")
                    continue

                writer.writerow(data)
                log_msg = f"[OK]   ID {i} — {data.get('name')}\n"
                f_log.write(log_msg)
                print(log_msg.strip())

            except Exception as e:
                f_log.write(f"[ERROR] ID {i} — {repr(e)}\n")
                f_log.write(traceback.format_exc() + "\n")
                continue

            sleep(0.3)

