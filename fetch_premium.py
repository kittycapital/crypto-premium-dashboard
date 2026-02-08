"""
프리미엄 대시보드 - 30분 스냅샷 수집기
- 업비트: KRW 전체 시세
- 빗썸: KRW 전체 시세
- Coinbase: USD 환율 (Binance는 GitHub Actions 차단 → Coinbase를 서버 기준으로)
- 환율: 업비트 BTC KRW / Coinbase BTC USD 로 역산
- 클라이언트에서는 Binance 기준 실시간 계산
"""

import json
import os
import time
import urllib.request
from datetime import datetime, timezone, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
HISTORY_DIR = os.path.join(DATA_DIR, "history")
COINS_FILE = os.path.join(DATA_DIR, "coins.json")

KST = timezone(timedelta(hours=9))


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


def get_upbit_prices():
    """업비트 KRW 마켓 전체 시세"""
    print("[1/4] 업비트 KRW 시세...")
    # 먼저 KRW 마켓 목록
    markets_data = api_get("https://api.upbit.com/v1/market/all?isDetails=false")
    if not markets_data:
        return {}
    
    krw_markets = [m["market"] for m in markets_data if m["market"].startswith("KRW-")]
    if not krw_markets:
        return {}
    
    # 200개씩 나눠서 호출
    prices = {}
    for i in range(0, len(krw_markets), 200):
        batch = krw_markets[i:i+200]
        query = ",".join(batch)
        data = api_get(f"https://api.upbit.com/v1/ticker?markets={query}")
        if data:
            for t in data:
                symbol = t["market"].replace("KRW-", "")
                prices[symbol] = {
                    "krw": float(t.get("trade_price", 0)),
                    "change_24h": float(t.get("signed_change_rate", 0)) * 100,
                }
        time.sleep(0.5)
    
    print(f"  → {len(prices)}개 코인")
    return prices


def get_bithumb_prices():
    """빗썸 KRW 전체 시세"""
    print("[2/4] 빗썸 KRW 시세...")
    data = api_get("https://api.bithumb.com/public/ticker/ALL_KRW")
    if not data or data.get("status") != "0000":
        return {}
    
    prices = {}
    for symbol, info in data.get("data", {}).items():
        if symbol == "date":
            continue
        try:
            prices[symbol] = {
                "krw": float(info.get("closing_price", 0)),
            }
        except (ValueError, TypeError):
            pass
    
    print(f"  → {len(prices)}개 코인")
    return prices


def get_coinbase_rates():
    """Coinbase USD 환율 (서버 기준가)"""
    print("[3/4] Coinbase USD 시세...")
    data = api_get("https://api.coinbase.com/v2/exchange-rates?currency=USD")
    if not data:
        return {}
    
    rates = data.get("data", {}).get("rates", {})
    # rates는 1 USD = X coin 형태 → 뒤집어서 1 coin = Y USD
    prices = {}
    for coin, rate in rates.items():
        try:
            r = float(rate)
            if r > 0:
                prices[coin.upper()] = {"usd": 1.0 / r}
        except (ValueError, TypeError):
            pass
    
    print(f"  → {len(prices)}개 코인")
    return prices


def get_coingecko_coins():
    """CoinGecko 마켓캡 TOP 코인 목록 (이미지 등 메타데이터)"""
    print("[4/4] CoinGecko 코인 목록...")
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false"
    data = api_get(url)
    if not data:
        return {}
    
    coins = {}
    for c in data:
        symbol = c.get("symbol", "").upper()
        coins[symbol] = {
            "id": c["id"],
            "name": c.get("name", ""),
            "image": c.get("image", ""),
            "market_cap_rank": c.get("market_cap_rank", 999),
        }
    
    print(f"  → {len(coins)}개 코인")
    return coins


def calculate_premiums(upbit, bithumb, coinbase, cg_coins):
    """프리미엄 계산"""
    
    # 환율 역산: 업비트 BTC KRW / Coinbase BTC USD
    upbit_btc = upbit.get("BTC", {}).get("krw", 0)
    cb_btc = coinbase.get("BTC", {}).get("usd", 0)
    
    if upbit_btc > 0 and cb_btc > 0:
        usd_krw = upbit_btc / cb_btc
    else:
        usd_krw = 1450  # 기본값
    
    print(f"\n  환율 (역산): 1 USD = {usd_krw:.1f} KRW")
    
    # 모든 코인 매칭
    all_symbols = set()
    all_symbols.update(upbit.keys())
    all_symbols.update(bithumb.keys())
    
    results = []
    for symbol in all_symbols:
        cb = coinbase.get(symbol, {}).get("usd", 0)
        up = upbit.get(symbol, {}).get("krw", 0)
        bt = bithumb.get(symbol, {}).get("krw", 0)
        
        if cb <= 0:
            continue
        
        coin_info = cg_coins.get(symbol, {})
        
        entry = {
            "symbol": symbol,
            "name": coin_info.get("name", symbol),
            "image": coin_info.get("image", ""),
            "market_cap_rank": coin_info.get("market_cap_rank", 999),
            "reference_usd": round(cb, 6),
            "usd_krw": round(usd_krw, 2),
        }
        
        # 업비트 프리미엄
        if up > 0:
            up_usd = up / usd_krw
            entry["upbit_krw"] = round(up, 2)
            entry["upbit_premium"] = round((up_usd - cb) / cb * 100, 2)
        
        # 빗썸 프리미엄
        if bt > 0:
            bt_usd = bt / usd_krw
            entry["bithumb_krw"] = round(bt, 2)
            entry["bithumb_premium"] = round((bt_usd - cb) / cb * 100, 2)
        
        results.append(entry)
    
    # 마켓캡 순 정렬
    results.sort(key=lambda x: x.get("market_cap_rank", 999))
    return results, usd_krw


def save_snapshot(results, usd_krw):
    """30분 스냅샷 저장"""
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    snapshot_file = os.path.join(HISTORY_DIR, f"{today}.json")
    
    # 기존 파일 로드
    daily_data = []
    if os.path.exists(snapshot_file):
        try:
            with open(snapshot_file) as f:
                daily_data = json.load(f)
        except:
            daily_data = []
    
    # 스냅샷 추가
    snap = {
        "timestamp": timestamp,
        "usd_krw": round(usd_krw, 2),
        "coins": {}
    }
    
    for r in results:
        s = r["symbol"]
        entry = {"ref": r.get("reference_usd", 0)}
        if "upbit_premium" in r:
            entry["up"] = r["upbit_premium"]
        if "bithumb_premium" in r:
            entry["bt"] = r["bithumb_premium"]
        snap["coins"][s] = entry
    
    daily_data.append(snap)
    
    with open(snapshot_file, "w") as f:
        json.dump(daily_data, f, separators=(',', ':'))
    
    print(f"  → 스냅샷 저장: {snapshot_file} ({len(daily_data)}건)")


def save_coins_json(results, usd_krw):
    """최신 데이터 저장"""
    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "usd_krw": round(usd_krw, 2),
        "coins": results,
    }
    
    with open(COINS_FILE, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"  → coins.json 저장: {len(results)}개 코인")


def load_history(days=30):
    """최근 N일 히스토리 로드"""
    history = []
    if not os.path.exists(HISTORY_DIR):
        return history
    
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    files = sorted([f for f in os.listdir(HISTORY_DIR) if f.endswith(".json") and f >= cutoff + ".json"])
    
    for fname in files:
        try:
            with open(os.path.join(HISTORY_DIR, fname)) as f:
                daily = json.load(f)
                history.extend(daily)
        except:
            pass
    
    return history


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(HISTORY_DIR, exist_ok=True)
    
    print("=" * 50)
    print("프리미엄 대시보드 스냅샷 수집")
    print("=" * 50)
    
    upbit = get_upbit_prices()
    time.sleep(1)
    bithumb = get_bithumb_prices()
    time.sleep(1)
    coinbase = get_coinbase_rates()
    time.sleep(1)
    cg_coins = get_coingecko_coins()
    
    if not coinbase:
        print("[!] Coinbase 실패, 종료")
        return
    
    results, usd_krw = calculate_premiums(upbit, bithumb, coinbase, cg_coins)
    
    save_snapshot(results, usd_krw)
    save_coins_json(results, usd_krw)
    
    # 요약
    with_upbit = [r for r in results if "upbit_premium" in r]
    with_bithumb = [r for r in results if "bithumb_premium" in r]
    
    print(f"\n✅ 완료!")
    print(f"   업비트: {len(with_upbit)}개, 빗썸: {len(with_bithumb)}개")
    print(f"   환율: 1 USD = {usd_krw:.1f} KRW")
    
    if with_upbit:
        top = max(with_upbit, key=lambda x: x["upbit_premium"])
        bot = min(with_upbit, key=lambda x: x["upbit_premium"])
        print(f"   🔺 최고: {top['symbol']} +{top['upbit_premium']:.1f}%")
        print(f"   🔻 최저: {bot['symbol']} {bot['upbit_premium']:.1f}%")


if __name__ == "__main__":
    main()
