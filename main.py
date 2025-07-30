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
    return " ".join(s.replace("\xa0", " ").split())


def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9"
    }
    resp = requests.get(url, headers=headers, timeout=20)
    resp.raise_for_status()
    return resp.text


def parse_name_from_page(soup: BeautifulSoup) -> Optional[str]:
    # Новый вариант: название в div с классом GameHeader_profile_header__q_PID
    div = soup.find("div", class_=re.compile(r"GameHeader_profile_header__.*"))
    if div and div.get_text(strip=True):
        return div.get_text(strip=True)
    # Резерв – JSON-LD
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


def parse_times_from_page(soup: BeautifulSoup) -> Dict[str, Optional[str]]:
    result = {v: None for v in TIME_LABELS.values()}

    for label, key in TIME_LABELS.items():
        label_node = soup.find(string=re.compile(rf"^{re.escape(label)}$", re.I))
        if label_node:
            # Ищем ближайший элемент с временем
            container = label_node.find_parent()
            if container:
                for s in container.stripped_strings:
                    if s.strip() != label and re.search(r"(Hour|Minute)", s, re.I):
                        result[key] = normalize_time_text(s)
                        break
            if not result[key]:
                sibling = label_node.find_next(string=re.compile(r"(Hour|Minute)", re.I))
                if sibling:
                    result[key] = normalize_time_text(sibling.strip())
    return result


def parse_hltb_game(url: str) -> Dict[str, Optional[str]]:
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
        "https://howlongtobeat.com/game/166252",
        "https://howlongtobeat.com/game/7231",
        "https://howlongtobeat.com/game/17250",
    ]

    results = []
    for url in urls:
        data = parse_hltb_game(url)
        results.append(data)

    print(json.dumps(results, ensure_ascii=False, indent=2))

