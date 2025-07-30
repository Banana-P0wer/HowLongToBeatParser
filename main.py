import re
import json
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict

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


def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9"
    }
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    return resp.text


def parse_name_from_page(soup: BeautifulSoup) -> Optional[str]:
    div = soup.find("div", class_=re.compile(r"GameHeader_profile_header__.*"))
    if div and div.get_text(strip=True):
        return div.get_text(strip=True)
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
    """Преобразует строку времени в часы (float)."""
    text = text.strip().lower().replace('\xa0', ' ')

    # 43½ Hours → 43.5
    if match := re.match(r"(\d+)\s*½\s*hour", text):
        return float(match.group(1)) + 0.5

    # ½ Hours → 0.5
    if re.match(r"^½\s*hour", text):
        return 0.5

    # 18 Mins или 18 Minutes → 0.3
    if match := re.match(r"(\d+)\s*(min|minute)", text):
        return round(int(match.group(1)) / 60, 2)

    # 43 Hours или 1 Hour → 43
    if match := re.match(r"(\d+)\s*hour", text):
        return float(match.group(1))

    # 1h 30m
    if match := re.match(r"(\d+)\s*h\s*(\d+)\s*m", text):
        hours = int(match.group(1))
        minutes = int(match.group(2))
        return round(hours + minutes / 60, 2)

    # fallback: нет совпадения
    return None


def parse_times_from_page(soup: BeautifulSoup) -> Dict[str, Optional[float]]:
    result = {v: None for v in TIME_LABELS.values()}

    for label, key in TIME_LABELS.items():
        label_node = soup.find(string=re.compile(rf"^{re.escape(label)}$", re.I))
        if label_node:
            container = label_node.find_parent()
            time_text = None
            if container:
                for s in container.stripped_strings:
                    if s.strip() != label and re.search(r"(Hour|Minute|Min)", s, re.I):
                        time_text = normalize_time_text(s)
                        break
            if not time_text:
                sibling = label_node.find_next(string=re.compile(r"(Hour|Minute|Min)", re.I))
                if sibling:
                    time_text = normalize_time_text(sibling.strip())
            if time_text:
                result[key] = parse_hours(time_text)
    return result


def parse_hltb_game(url: str) -> Dict[str, Optional[float]]:
    game_id = extract_id_from_url(url)
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    name = parse_name_from_page(soup)
    times = parse_times_from_page(soup)

    return {
        "id": str(game_id),
        "name": name,
        **times
    }


if __name__ == "__main__":
    urls = [
        "https://howlongtobeat.com/game/82645",  # пример с Mins
        "https://howlongtobeat.com/game/7231",
        "https://howlongtobeat.com/game/17250",
    ]

    results = []
    for url in urls:
        data = parse_hltb_game(url)
        results.append(data)

    print(json.dumps(results, ensure_ascii=False, indent=2))
