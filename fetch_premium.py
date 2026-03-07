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
            "coin_id": c.get("id", ""),
            "symbol": sym,
            "name": c.get("name", sym),
            "image": c.get("image", ""),
            "market_cap_rank": c.get("market_cap_rank", 999),
        }
    log(f"  → 총 {len(coins)}개 코인")
    return coins


# ═══════════════════════════════════════
# 1b. 거래소 매핑 로드
# ═══════════════════════════════════════
def load_exchange_map():
    """exchange_map.json 로드 — 심볼 충돌 방지용"""
    map_path = DATA_DIR / "exchange_map.json"
    if not map_path.exists():
        log("⚠ exchange_map.json 없음 — 수동 충돌 목록 사용")
        return None
    try:
        with open(map_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        exchanges = data.get("exchanges", {})
        total = sum(len(m) for m in exchanges.values())
        log(f"✓ exchange_map.json 로드: {total}개 매핑")
        return exchanges
    except Exception as e:
        log(f"⚠ exchange_map.json 로드 실패: {e}")
        return None


# ═══════════════════════════════════════
# 수동 오버라이드 (exchange_map.json보다 우선)
# CoinGecko API 매핑 오류를 수동으로 보정
# 형식: {거래소: {심볼: 올바른_coin_id 또는 False(차단)}}
#   - coin_id 지정: 해당 coin_id와 일치할 때만 통과
#   - False: 무조건 차단 (거래소에서 다른 코인을 거래하는 경우)
# ═══════════════════════════════════════
MANUAL_OVERRIDES = {
    "bithumb": {
        "LIT": False,       # 빗썸 LIT = Litentry, CoinGecko TOP200 LIT = Lighter → 차단
    },
    "upbit": {},
    "coinbase": {
        "MNT": False,       # 코인베이스 MNT ≠ Mantle → 차단
    },
}


def is_valid_match(exchange_name, symbol, coin_id, exchange_map):
    """거래소 심볼이 CoinGecko coin_id와 일치하는지 검증
    우선순위: MANUAL_OVERRIDES > exchange_map.json > 기본 통과
    """
    # 1) 수동 오버라이드 — 최우선
    overrides = MANUAL_OVERRIDES.get(exchange_name, {})
    if symbol in overrides:
        override_val = overrides[symbol]
        if override_val is False:
            log(f"  ✗ 수동 차단: {exchange_name}/{symbol} (CoinGecko/{coin_id}와 다른 코인)")
            return False
        # 특정 coin_id만 허용
        if override_val != coin_id:
            log(f"  ✗ 수동 불일치: {exchange_name}/{symbol} → {override_val} ≠ CoinGecko/{coin_id}")
            return False
        return True

    # 2) exchange_map.json 기반 검증
    if exchange_map is not None:
        ex_map = exchange_map.get(exchange_name, {})
        if symbol in ex_map:
            mapped_id = ex_map[symbol]
            if mapped_id != coin_id:
                log(f"  ✗ 심볼 충돌: {exchange_name}/{symbol} → {mapped_id} ≠ CoinGecko/{coin_id}")
                return False
            return True
        return True  # 매핑에 없는 심볼은 통과

    return True


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
# 6. 바이낸스 — USD 가격 (히스토리 기준가용)
# ═══════════════════════════════════════
def fetch_binance():
    """바이낸스 USDT 페어 가격"""
    log("🌐 바이낸스 가격 조회...")
    data = safe_get("https://api.binance.com/api/v3/ticker/price")
    if not data:
        return {}

    prices = {}
    for t in data:
        if t["symbol"].endswith("USDT"):
            sym = t["symbol"].replace("USDT", "")
            prices[sym] = round(float(t["price"]), 8)

    log(f"  ✓ 바이낸스 {len(prices)}개 코인")
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

    # 2b) 거래소 매핑 로드
    exchange_map = load_exchange_map()

    # 3) 업비트
    upbit_prices = fetch_upbit(symbols)
    skipped = 0
    for sym, price in upbit_prices.items():
        if sym in coins:
            if is_valid_match("upbit", sym, coins[sym]["coin_id"], exchange_map):
                coins[sym]["upbit_krw"] = price
            else:
                skipped += 1
    if skipped:
        log(f"  ⚠ 업비트 {skipped}개 심볼 충돌 건너뜀")

    # 4) 빗썸
    bithumb_prices = fetch_bithumb(symbols)
    skipped = 0
    for sym, price in bithumb_prices.items():
        if sym in coins:
            if is_valid_match("bithumb", sym, coins[sym]["coin_id"], exchange_map):
                coins[sym]["bithumb_krw"] = price
            else:
                skipped += 1
    if skipped:
        log(f"  ⚠ 빗썸 {skipped}개 심볼 충돌 건너뜀")

    # 5) 코인베이스 (주요 코인만 — rate limit 때문에)
    # 마켓캡 상위 50개만
    top_symbols = sorted(symbols, key=lambda s: coins[s].get("market_cap_rank", 999))[:50]
    coinbase_prices = fetch_coinbase(top_symbols)
    skipped = 0
    for sym, price in coinbase_prices.items():
        if sym in coins:
            if is_valid_match("coinbase", sym, coins[sym]["coin_id"], exchange_map):
                coins[sym]["coinbase_usd"] = price
            else:
                skipped += 1
    if skipped:
        log(f"  ⚠ 코인베이스 {skipped}개 심볼 충돌 건너뜀")

    # 6) 바이낸스
    binance_prices = fetch_binance()
    for sym, price in binance_prices.items():
        if sym in coins:
            coins[sym]["binance_usd"] = price

    # 7) JSON 저장 (coin_id는 내부용이므로 제거)
    clean_coins = []
    for c in coins.values():
        cc = {k: v for k, v in c.items() if k != "coin_id"}
        clean_coins.append(cc)

    output = {
        "usd_krw": round(usd_krw, 2),
        "coins": clean_coins,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    output_path = DATA_DIR / "coins.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False)

    # 7) 히스토리 스냅샷 저장 (premium-all.html 추이 차트용)
    history_dir = DATA_DIR / "history"
    history_dir.mkdir(exist_ok=True)
    now_utc = datetime.now(timezone.utc)
    today_file = history_dir / f"{now_utc.strftime('%Y-%m-%d')}.json"

    # 기존 파일 있으면 로드, 없으면 빈 배열
    history_entries = []
    if today_file.exists():
        try:
            with open(today_file, "r", encoding="utf-8") as f:
                history_entries = json.load(f)
        except (json.JSONDecodeError, Exception):
            history_entries = []

    # 스냅샷 생성: {timestamp, usd_krw, coins: {SYM: {up_krw, bt_krw, cb_usd, ref_usd}}}
    # 기준가(ref_usd): binance > coinbase > KRW역산 순으로 결정
    ref_prices = {}
    for sym, c in coins.items():
        if "binance_usd" in c:
            ref_prices[sym] = c["binance_usd"]
        elif "coinbase_usd" in c:
            ref_prices[sym] = c["coinbase_usd"]
        elif "upbit_krw" in c and usd_krw > 0:
            ref_prices[sym] = round(c["upbit_krw"] / usd_krw, 6)
        elif "bithumb_krw" in c and usd_krw > 0:
            ref_prices[sym] = round(c["bithumb_krw"] / usd_krw, 6)

    snap_coins = {}
    for sym, c in coins.items():
        entry = {}
        if "upbit_krw" in c:
            entry["up_krw"] = c["upbit_krw"]
        if "bithumb_krw" in c:
            entry["bt_krw"] = c["bithumb_krw"]
        if "coinbase_usd" in c:
            entry["cb_usd"] = c["coinbase_usd"]
        if "binance_usd" in c:
            entry["bn_usd"] = c["binance_usd"]
        if sym in ref_prices:
            entry["ref_usd"] = ref_prices[sym]
        if entry:
            snap_coins[sym] = entry

    snapshot = {
        "timestamp": now_utc.isoformat(),
        "usd_krw": round(usd_krw, 2),
        "coins": snap_coins,
    }
    history_entries.append(snapshot)

    with open(today_file, "w", encoding="utf-8") as f:
        json.dump(history_entries, f, ensure_ascii=False)

    # 30일 이전 히스토리 파일 자동 정리
    cutoff = now_utc.strftime('%Y-%m-%d')
    from datetime import timedelta
    cutoff_date = (now_utc - timedelta(days=30)).strftime('%Y-%m-%d')
    for old_file in history_dir.glob("*.json"):
        if old_file.stem < cutoff_date:
            old_file.unlink()
            log(f"  🗑 오래된 히스토리 삭제: {old_file.name}")

    log(f"\n✅ 저장 완료: {output_path}")
    log(f"   코인: {len(output['coins'])}개")
    log(f"   환율: {usd_krw:.0f}원")
    log(f"   업비트: {len(upbit_prices)}개")
    log(f"   빗썸: {len(bithumb_prices)}개")
    log(f"   코인베이스: {len(coinbase_prices)}개")
    log(f"   바이낸스: {len(binance_prices)}개")
    log(f"   히스토리: {today_file.name} ({len(history_entries)}개 스냅샷)")


if __name__ == "__main__":
    main()
