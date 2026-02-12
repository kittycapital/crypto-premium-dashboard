"""
프리미엄 대시보드 - 서버 사이드 데이터 수집 (30분 간격)
- CoinGecko: TOP 200 코인 메타데이터 (이름, 이미지, 랭크)
- 업비트: KRW 전체 시세
- 빗썸: KRW 전체 시세
- Coinbase: USD 시세
- Binance: GitHub Actions에서 차단 → 클라이언트에서 실시간 호출

coins.json에 모든 거래소 가격을 저장하고,
클라이언트에서 Binance 가격만 실시간으로 가져와서 프리미엄 계산
"""

import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
COINS_FILE = os.path.join(DATA_DIR, "coins.json")


def api_get(url, retries=3, delay=2):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json"
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            print(f"  [!] Attempt {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    return None


def get_coingecko_top():
    """CoinGecko 마켓캡 TOP 200"""
    print("[1/4] CoinGecko TOP 200 메타데이터...")
    data = api_get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=200&page=1&sparkline=false")
    if not data:
        return {}
    coins = {}
    for c in data:
        sym = c.get("symbol", "").upper()
        coins[sym] = {
            "id": c["id"],
            "name": c.get("name", ""),
            "image": c.get("image", ""),
            "market_cap_rank": c.get("market_cap_rank", 999),
        }
    print(f"  → {len(coins)}개 코인")
    return coins


def get_upbit_prices():
    """업비트 KRW 마켓 전체"""
    print("[2/4] 업비트 KRW 시세...")
    markets_data = api_get("https://api.upbit.com/v1/market/all?isDetails=false")
    if not markets_data:
        return {}

    krw_markets = [m["market"] for m in markets_data if m["market"].startswith("KRW-")]
    prices = {}

    for i in range(0, len(krw_markets), 200):
        batch = krw_markets[i:i+200]
        query = ",".join(batch)
        data = api_get(f"https://api.upbit.com/v1/ticker?markets={query}")
        if data:
            for t in data:
                sym = t["market"].replace("KRW-", "")
                prices[sym] = float(t.get("trade_price", 0))
        time.sleep(0.5)

    print(f"  → {len(prices)}개 코인")
    return prices


def get_bithumb_prices():
    """빗썸 KRW 전체"""
    print("[3/4] 빗썸 KRW 시세...")
    data = api_get("https://api.bithumb.com/public/ticker/ALL_KRW")
    if not data or data.get("status") != "0000":
        return {}

    prices = {}
    for sym, info in data.get("data", {}).items():
        if sym == "date":
            continue
        try:
            p = float(info.get("closing_price", 0))
            if p > 0:
                prices[sym] = p
        except (ValueError, TypeError):
            pass

    print(f"  → {len(prices)}개 코인")
    return prices


def get_coinbase_prices():
    """Coinbase USD 시세"""
    print("[4/4] Coinbase USD 시세...")
    data = api_get("https://api.coinbase.com/v2/exchange-rates?currency=USD")
    if not data:
        return {}

    rates = data.get("data", {}).get("rates", {})
    prices = {}
    for coin, rate in rates.items():
        try:
            r = float(rate)
            if r > 0:
                prices[coin.upper()] = round(1.0 / r, 8)
        except (ValueError, TypeError):
            pass

    print(f"  → {len(prices)}개 코인")
    return prices


def build_coins_json(cg_meta, upbit, bithumb, coinbase):
    """coins.json 빌드 - CoinGecko TOP 200 기준 필터링"""

    # 환율 역산: 업비트 BTC / Coinbase BTC
    up_btc = upbit.get("BTC", 0)
    cb_btc = coinbase.get("BTC", 0)
    usd_krw = up_btc / cb_btc if up_btc > 0 and cb_btc > 0 else 1450
    print(f"\n  환율 (역산): 1 USD = {usd_krw:.1f} KRW")

    # CoinGecko TOP 100 심볼만 사용 (이상한 매칭 방지)
    valid_symbols = set(cg_meta.keys())

    coins = []
    for sym in valid_symbols:
        meta = cg_meta[sym]
        entry = {
            "symbol": sym,
            "name": meta.get("name", sym),
            "image": meta.get("image", ""),
            "market_cap_rank": meta.get("market_cap_rank", 999),
        }

        # 업비트 KRW
        if sym in upbit:
            entry["upbit_krw"] = upbit[sym]

        # 빗썸 KRW
        if sym in bithumb:
            entry["bithumb_krw"] = bithumb[sym]

        # Coinbase USD
        if sym in coinbase:
            entry["coinbase_usd"] = round(coinbase[sym], 6)

        # 거래소 하나라도 있어야 포함
        if "upbit_krw" in entry or "bithumb_krw" in entry or "coinbase_usd" in entry:
            coins.append(entry)

    coins.sort(key=lambda x: x.get("market_cap_rank", 999))
    return coins, usd_krw


def save_snapshot(coins, usd_krw):
    """30분 히스토리 스냅샷"""
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    snapshot_file = os.path.join(HISTORY_DIR, f"{today}.json")

    daily_data = []
    if os.path.exists(snapshot_file):
        try:
            with open(snapshot_file) as f:
                daily_data = json.load(f)
        except:
            daily_data = []

    snap = {"timestamp": timestamp, "usd_krw": round(usd_krw, 2), "coins": {}}
    for c in coins:
        sym = c["symbol"]
        entry = {}
        if "upbit_krw" in c:
            entry["up_krw"] = c["upbit_krw"]
        if "bithumb_krw" in c:
            entry["bt_krw"] = c["bithumb_krw"]
        if "coinbase_usd" in c:
            entry["cb_usd"] = c["coinbase_usd"]
        if entry:
            snap["coins"][sym] = entry

    daily_data.append(snap)

    with open(snapshot_file, "w") as f:
        json.dump(daily_data, f, separators=(',', ':'))

    print(f"  → 스냅샷: {snapshot_file} ({len(daily_data)}건)")

    # 30일 이전 히스토리 정리
    cutoff = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    for fname in os.listdir(HISTORY_DIR):
        if fname.endswith(".json") and fname < cutoff + ".json":
            os.remove(os.path.join(HISTORY_DIR, fname))
            print(f"  → 삭제 (30일 경과): {fname}")


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(HISTORY_DIR, exist_ok=True)

    print("=" * 50)
    print("프리미엄 대시보드 - 서버 데이터 수집")
    print("=" * 50)

    cg_meta = get_coingecko_top()
    time.sleep(1)
    upbit = get_upbit_prices()
    time.sleep(1)
    bithumb = get_bithumb_prices()
    time.sleep(1)
    coinbase = get_coinbase_prices()

    if not cg_meta:
        print("[!] CoinGecko 실패, 종료")
        return

    coins, usd_krw = build_coins_json(cg_meta, upbit, bithumb, coinbase)

    # coins.json 저장
    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "usd_krw": round(usd_krw, 2),
        "coins": coins,
    }

    with open(COINS_FILE, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # 스냅샷 저장
    save_snapshot(coins, usd_krw)

    # 요약
    with_up = sum(1 for c in coins if "upbit_krw" in c)
    with_bt = sum(1 for c in coins if "bithumb_krw" in c)
    with_cb = sum(1 for c in coins if "coinbase_usd" in c)

    print(f"\n✅ 완료! {len(coins)}개 코인")
    print(f"   업비트: {with_up}개 · 빗썸: {with_bt}개 · Coinbase: {with_cb}개")
    print(f"   환율: 1 USD = {usd_krw:.1f} KRW")


if __name__ == "__main__":
    main()
