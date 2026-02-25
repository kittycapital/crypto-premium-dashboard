"""
거래소별 심볼 → CoinGecko coin_id 매핑 생성기

CoinGecko /exchanges/{id}/tickers API를 사용해
업비트·빗썸·코인베이스의 심볼이 실제 어떤 CoinGecko 코인인지 매핑합니다.

출력: data/exchange_map.json
수동 실행: python generate_exchange_map.py
(가끔 실행하면 됨 — 신규 상장 시)
"""

import json
import time
import requests
from pathlib import Path
from datetime import datetime, timezone

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {"Accept": "application/json", "User-Agent": "HerdVibe-Mapper/1.0"}

EXCHANGES = {
    "upbit": "upbit",
    "bithumb": "bithumb",
    "coinbase": "gdax",  # CoinGecko uses 'gdax' for Coinbase
}


def fetch_exchange_tickers(exchange_id):
    """CoinGecko /exchanges/{id}/tickers 전체 페이지 수집"""
    all_tickers = []
    page = 1
    while True:
        url = f"https://api.coingecko.com/api/v3/exchanges/{exchange_id}/tickers"
        try:
            r = requests.get(url, params={"page": page}, headers=HEADERS, timeout=30)
            if r.status_code == 429:
                print(f"  Rate limited, waiting 60s...")
                time.sleep(60)
                r = requests.get(url, params={"page": page}, headers=HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            tickers = data.get("tickers", [])
            if not tickers:
                break
            all_tickers.extend(tickers)
            print(f"  {exchange_id} page {page}: {len(tickers)} tickers")
            page += 1
            time.sleep(1.5)  # Rate limit
        except Exception as e:
            print(f"  Error on {exchange_id} page {page}: {e}")
            break
    return all_tickers


def build_mapping():
    """거래소별 심볼 → coin_id 매핑 생성"""
    mapping = {}

    for name, cg_id in EXCHANGES.items():
        print(f"\n{'='*40}")
        print(f"Fetching {name} ({cg_id})...")
        print(f"{'='*40}")

        tickers = fetch_exchange_tickers(cg_id)
        exchange_map = {}

        for t in tickers:
            symbol = t.get("base", "").upper()
            coin_id = t.get("coin_id", "")
            target = t.get("target", "")

            # 업비트/빗썸: KRW 마켓만
            if name in ("upbit", "bithumb"):
                if target != "KRW":
                    continue

            # 코인베이스: USD 마켓만
            if name == "coinbase":
                if target != "USD":
                    continue

            if symbol and coin_id:
                if symbol in exchange_map and exchange_map[symbol] != coin_id:
                    print(f"  CONFLICT: {symbol} → {exchange_map[symbol]} vs {coin_id}")
                exchange_map[symbol] = coin_id

        mapping[name] = exchange_map
        print(f"  Total: {len(exchange_map)} symbols mapped")

    return mapping


def find_conflicts(mapping):
    """거래소 간 같은 심볼이 다른 coin_id를 가리키는 경우 찾기"""
    all_symbols = set()
    for ex_map in mapping.values():
        all_symbols.update(ex_map.keys())

    conflicts = []
    for sym in sorted(all_symbols):
        coin_ids = {}
        for ex_name, ex_map in mapping.items():
            if sym in ex_map:
                coin_ids[ex_name] = ex_map[sym]

        unique_ids = set(coin_ids.values())
        if len(unique_ids) > 1:
            conflicts.append({"symbol": sym, "mappings": coin_ids})

    return conflicts


def main():
    print("Exchange Symbol Mapping Generator")
    print("=" * 50)

    mapping = build_mapping()

    # 충돌 감지
    conflicts = find_conflicts(mapping)
    if conflicts:
        print(f"\n⚠ {len(conflicts)} symbol conflicts found:")
        for c in conflicts:
            print(f"  {c['symbol']}: {c['mappings']}")

    # 저장
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "exchanges": mapping,
        "conflicts": conflicts,
    }

    out_path = DATA_DIR / "exchange_map.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nSaved to {out_path}")
    total = sum(len(m) for m in mapping.values())
    print(f"Total mappings: {total}")
    print(f"Conflicts: {len(conflicts)}")


if __name__ == "__main__":
    main()
