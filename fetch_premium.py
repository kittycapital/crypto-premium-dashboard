"""
프리미엄 시그널 — 데이터 수집 스크립트
CoinGecko TOP 200 + 업비트 + 빗썸 + 코인베이스 가격 수집
→ data/coins.json 생성

사용법:
    pip install requests
    python fetch_premium.py
"""

import os
import json
import time
import requests
from pathlib import Path
from datetime import datetime, timezone

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {"Accept": "application/json"}
COINGECKO_BASE = "https://api.coingecko.com/api/v3"


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}")


def safe_get(url, params=None, timeout=20):
    """Rate-limit aware GET request"""
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        if r.status_code == 429:
            log("  ⚠ Rate limited, waiting 60s...")
            time.sleep(60)
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log(f"  ✗ {url[:80]}... → {e}")
        return None


# ═══════════════════════════════════════
# 1. CoinGecko — TOP 200 마켓캡 코인
# ═══════════════════════════════════════
def fetch_coingecko():
    """CoinGecko TOP 200 coins by market cap"""
    log("📊 CoinGecko TOP 200 가져오는 중...")
    all_coins = []
    for page in range(1, 3):  # page 1-2, 100 each = 200
        data = safe_get(f"{COINGECKO_BASE}/coins/markets", params={
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": page,
            "sparkline": "false",
        })
        if data:
            all_coins.extend(data)
            log(f"  ✓ 페이지 {page}: {len(data)}개")
        time.sleep(2)  # Rate limit

    coins = {}
    for c in all_coins:
        sym = c.get("symbol", "").upper()
        coins[sym] = {
            "symbol": sym,
            "name": c.get("name", sym),
            "image": c.get("image", ""),
            "market_cap_rank": c.get("market_cap_rank", 999),
        }
    log(f"  → 총 {len(coins)}개 코인")
    return coins


# ═══════════════════════════════════════
# 2. 환율 — USD/KRW
# ═══════════════════════════════════════
def fetch_usd_krw():
    """USD/KRW 환율 (여러 소스 시도)"""
    log("💱 USD/KRW 환율 조회...")

    # 방법 1: exchangerate-api
    try:
        data = safe_get("https://open.er-api.com/v6/latest/USD")
        if data and "rates" in data:
            rate = data["rates"].get("KRW", 0)
            if rate > 0:
                log(f"  ✓ USD/KRW = {rate:.2f} (exchangerate-api)")
                return rate
    except Exception:
        pass

    # 방법 2: frankfurter
    try:
        data = safe_get("https://api.frankfurter.app/latest?from=USD&to=KRW")
        if data and "rates" in data:
            rate = data["rates"].get("KRW", 0)
            if rate > 0:
                log(f"  ✓ USD/KRW = {rate:.2f} (frankfurter)")
                return rate
    except Exception:
        pass

    log("  ⚠ 환율 조회 실패, 기본값 1450 사용")
    return 1450.0


# ═══════════════════════════════════════
# 3. 업비트 — KRW 가격
# ═══════════════════════════════════════
def fetch_upbit(symbols):
    """업비트 KRW 마켓 가격"""
    log("🇰🇷 업비트 가격 조회...")
    # 업비트 마켓 코드 조회
    markets_data = safe_get("https://api.upbit.com/v1/market/all", params={"isDetails": "false"})
    if not markets_data:
        return {}

    # KRW 마켓만 필터
    krw_markets = {}
    for m in markets_data:
        code = m.get("market", "")
        if code.startswith("KRW-"):
            sym = code.replace("KRW-", "")
            krw_markets[sym] = code

    # 심볼 매칭
    target_codes = []
    for sym in symbols:
        if sym in krw_markets:
            target_codes.append(krw_markets[sym])

    if not target_codes:
        return {}

    # 가격 조회 (최대 100개씩)
    prices = {}
    for i in range(0, len(target_codes), 100):
        batch = target_codes[i:i + 100]
        data = safe_get("https://api.upbit.com/v1/ticker", params={"markets": ",".join(batch)})
        if data:
            for t in data:
                sym = t["market"].replace("KRW-", "")
                prices[sym] = t.get("trade_price", 0)
        time.sleep(0.5)

    log(f"  ✓ 업비트 {len(prices)}개 코인")
    return prices


# ═══════════════════════════════════════
# 4. 빗썸 — KRW 가격
# ═══════════════════════════════════════
def fetch_bithumb(symbols):
    """빗썸 KRW 가격"""
    log("🇰🇷 빗썸 가격 조회...")
    data = safe_get("https://api.bithumb.com/public/ticker/ALL_KRW")
    if not data or data.get("status") != "0000":
        log("  ✗ 빗썸 API 실패")
        return {}

    prices = {}
    tickers = data.get("data", {})
    for sym in symbols:
        if sym in tickers and isinstance(tickers[sym], dict):
            price = float(tickers[sym].get("closing_price", 0))
            if price > 0:
                prices[sym] = price

    log(f"  ✓ 빗썸 {len(prices)}개 코인")
    return prices


# ═══════════════════════════════════════
# 5. 코인베이스 — USD 가격
# ═══════════════════════════════════════
def fetch_coinbase(symbols):
    """코인베이스 USD 가격"""
    log("🇺🇸 코인베이스 가격 조회...")
    prices = {}
    for sym in symbols:
        data = safe_get(f"https://api.coinbase.com/v2/prices/{sym}-USD/spot")
        if data and "data" in data:
            try:
                prices[sym] = float(data["data"]["amount"])
            except (KeyError, ValueError):
                pass
        time.sleep(0.2)  # Rate limit

    log(f"  ✓ 코인베이스 {len(prices)}개 코인")
    return prices


# ═══════════════════════════════════════
# 메인
# ═══════════════════════════════════════
def main():
    log("=" * 50)
    log("프리미엄 시그널 — 데이터 수집 시작")
    log("=" * 50)

    # 1) CoinGecko TOP 200
    coins = fetch_coingecko()
    if not coins:
        log("✗ CoinGecko 데이터 없음, 종료")
        return

    symbols = list(coins.keys())

    # 2) 환율
    usd_krw = fetch_usd_krw()

    # 3) 업비트
    upbit_prices = fetch_upbit(symbols)
    for sym, price in upbit_prices.items():
        if sym in coins:
            coins[sym]["upbit_krw"] = price

    # 4) 빗썸
    bithumb_prices = fetch_bithumb(symbols)
    for sym, price in bithumb_prices.items():
        if sym in coins:
            coins[sym]["bithumb_krw"] = price

    # 5) 코인베이스 (주요 코인만 — rate limit 때문에)
    # 마켓캡 상위 50개만
    top_symbols = sorted(symbols, key=lambda s: coins[s].get("market_cap_rank", 999))[:50]
    coinbase_prices = fetch_coinbase(top_symbols)
    for sym, price in coinbase_prices.items():
        if sym in coins:
            coins[sym]["coinbase_usd"] = price

    # 6) JSON 저장
    output = {
        "usd_krw": round(usd_krw, 2),
        "coins": list(coins.values()),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    output_path = DATA_DIR / "coins.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False)

    log(f"\n✅ 저장 완료: {output_path}")
    log(f"   코인: {len(output['coins'])}개")
    log(f"   환율: {usd_krw:.0f}원")
    log(f"   업비트: {len(upbit_prices)}개")
    log(f"   빗썸: {len(bithumb_prices)}개")
    log(f"   코인베이스: {len(coinbase_prices)}개")


if __name__ == "__main__":
    main()
