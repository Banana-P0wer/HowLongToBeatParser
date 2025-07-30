import re
import json
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List

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
        return round(int(m.group(1)) / 60.0, 2)

    m = re.match(r"^(\d+)\s*hours?", text)
    if m:
        return float(m.group(1))

    m = re.match(r"^(\d+)\s*h\s*(\d+)\s*m", text)
    if m:
        hours = int(m.group(1))
        minutes = int(m.group(2))
        return round(hours + minutes / 60.0, 2)

    return None


def parse_times_from_page(soup: BeautifulSoup) -> Dict[str, Optional[float]]:
    result = {v: None for v in TIME_LABELS.values()}
    time_token_re = re.compile(r"(hours?|mins?|minutes?)", re.I)

    for label, key in TIME_LABELS.items():
        label_node = soup.find(string=re.compile(rf"^{re.escape(label)}$", re.I))
        if not label_node:
            continue

        time_text = None

        container = label_node.find_parent()
        if container:
            for s in container.stripped_strings:
                if s.strip() != label and time_token_re.search(s):
                    time_text = normalize_time_text(s)
                    break

        if not time_text:
            sibling = label_node.find_next(string=time_token_re)
            if sibling:
                time_text = normalize_time_text(str(sibling))

        if time_text:
            result[key] = parse_hours(time_text)

    return result


def parse_hltb_game(url: str) -> Optional[Dict[str, Any]]:
    html = fetch_html(url)
    if not html:
        return None  # «тихий» пропуск отсутствующих страниц и сетевых ошибок

    soup = BeautifulSoup(html, "html.parser")
    name = parse_name_from_page(soup)
    if not name:
        return None  # если имя не извлечено, тоже пропускаем запись

    times = parse_times_from_page(soup)
    try:
        game_id = extract_id_from_url(url)
    except ValueError:
        return None

    return {
        "id": str(game_id),
        "name": name,
        **times
    }


if __name__ == "__main__":
    urls: List[str] = [
        "https://howlongtobeat.com/game/10",  # несуществующий → будет тихо пропущен
        "https://howlongtobeat.com/game/82645",  # пример с Mins
        "https://howlongtobeat.com/game/7231",
        "https://howlongtobeat.com/game/17250",
    ]

    results = []
    for url in urls:
        data = parse_hltb_game(url)
        if data is None:
            continue
        results.append(data)

    print(json.dumps(results, ensure_ascii=False, indent=2))
