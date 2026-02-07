# HowLongToBeat parser (async, resilient, deduplicated)

Asynchronous, fault-tolerant web scraper for [howlongtobeat.com](https://howlongtobeat.com) collecting game statistics: completion times, multiplayer modes, release dates.

### [Kaggle link](https://www.kaggle.com/datasets/b4n4n4p0wer/how-long-to-beat-video-game-playtime-dataset)

[![Screenshot.png](https://i.postimg.cc/7L2DWGR8/Screenshot.png)](https://postimg.cc/Lq2rqXRN)
<sub>Source: howlongtobeat.com</sub>

## What this project does

Goes through pages like `https://howlongtobeat.com/game/<id>` (by ID) and extracts:
  - Game title
  - Content type (`Game`, `DLC`, `Multiplayer`)
  - Available completion times: `Main Story`, `Completionist`, `Co-op`, etc.
  - Release date (precision: year, month, or day)
  - Number of polled users for each time metric

Saves data to `hltb_dataset.csv` and writes logs about errors, skips, timeouts, and retries

## How to run

Install all deps
```bash
pip install -r requirements.txt
```
Run the parsing process. For example, for the command:
```bash
python main.py 1000 --start 1 --concurrency 8 --miss-threshold 200
```
- `1000` = how many IDs in a row to check
-	`--start 1` = which ID to start from
(if `--start` is not set, it will start from the last ID in the CSV + 1)
- `--concurrency 8` = up to 8 simultaneous HTTP requests
(these are not OS threads, but asynchronous tasks using asyncio)
- `--miss-threshold 200` = the number of failed attempts in a row. If 200 id in a row return a 404 error, the program will assume there are no more games and stop (since the id of the last added game on the site constantly changes, this method is much simpler than trying to determine the latest id every time).

The final range will be 1 to 1000 (inclusive).
These 1000 include skipped IDs and 404 pages, not only found games.



You can also run it without a limit:
```bash
python main.py "*" --concurrency 8
```
It will stop when it reaches the miss limit (default is 400 misses in a row).

## Features

- Sends requests in parallel (aiohttp + asyncio)
- Retries failed requests with delays
- Normalizes labels so CSV columns stay consistent
- Can resume without writing duplicate IDs
- CLI flags for ID count, start point, CSV path, and log path


## Technologies

- `Python 3.11`
- `aiohttp` — async HTTP requests
- `asyncio` — async tasks
- `BeautifulSoup4` — HTML parsing
- `CSV` — output format
- `Regex` — text parsing
