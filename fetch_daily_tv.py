#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_daily_tv.py (D-2 EMA -> D-1 EMA 출력)

목표:
- 전전날(D-2)의 EMA를 가져온 뒤
- 전날(D-1)의 OHLCV를 TradingView에서 가져와서
- 전날 EMA를 1-step 업데이트로 계산해 출력/저장

EMA 업데이트:
  alpha = 2/(span+1)
  EMA_D-1 = alpha*Close_D-1 + (1-alpha)*EMA_D-2

권장 운영:
- 스케줄러(UTC 00:xx)에 실행
- 전날(D-1) row를 DB에 UPSERT
- "prev_ema(=D-2 EMA)"는 DB에서 읽어오는 게 가장 정확
  (대안: history_csv에서 D-2 EMA 읽기)

필요:
  pip install tradingview-datafeed pandas
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Optional

import pandas as pd

# tvDatafeed 로거 소음 억제
for name in ["tvDatafeed", "tvDatafeed.main", "tvdatafeed", "tvdatafeed.main"]:
    logging.getLogger(name).setLevel(logging.CRITICAL)

DEFAULT_SYMBOLS = {
    "corn": ("CBOT", "ZC1!"),
    "soybean": ("CBOT", "ZM1!"),
    "wheat": ("CBOT", "ZW1!"),
}

NEED_OHLCV = ["time", "open", "high", "low", "close", "Volume"]


def _import_tvdatafeed():
    try:
        from tvDatafeed import TvDatafeed, Interval
        return TvDatafeed, Interval
    except Exception:
        from tvdatafeed import TvDatafeed, Interval
        return TvDatafeed, Interval


def _fetch_recent_bars(
    exchange: str,
    symbol: str,
    n_bars: int,
    tv_user: Optional[str],
    tv_pass: Optional[str],
    retries: int = 5,
    sleep_base: int = 2,
    debug: bool = False,
) -> pd.DataFrame:
    TvDatafeed, Interval = _import_tvdatafeed()
    tv = TvDatafeed(tv_user, tv_pass)

    df = None
    last_err = None
    for i in range(retries):
        try:
            df = tv.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.in_daily,
                n_bars=int(n_bars),
            )
            if df is not None and not df.empty:
                break
        except Exception as e:
            last_err = e
        time.sleep(sleep_base * (i + 1))

    if df is None or df.empty:
        if last_err is not None:
            print(f"[WARN] TradingView fetch failed: {last_err}", file=sys.stderr)
        return pd.DataFrame()

    if debug:
        print("[DEBUG] raw columns:", list(df.columns))
        print("[DEBUG] index name:", df.index.name)
        print("[DEBUG] head:\n", df.head(3))

    out = df.reset_index()

    # time 컬럼 통일
    if "datetime" in out.columns:
        out = out.rename(columns={"datetime": "time"})
    elif "date" in out.columns:
        out = out.rename(columns={"date": "time"})
    elif "time" not in out.columns:
        out = out.rename(columns={out.columns[0]: "time"})

    # volume -> Volume
    if "volume" in out.columns and "Volume" not in out.columns:
        out = out.rename(columns={"volume": "Volume"})

    # 혹시 대문자 컬럼이면 소문자로 보정
    for c in ["open", "high", "low", "close"]:
        if c not in out.columns and c.capitalize() in out.columns:
            out = out.rename(columns={c.capitalize(): c})

    missing = [c for c in ["time", "open", "high", "low", "close", "Volume"] if c not in out.columns]
    if missing:
        raise ValueError(f"TradingView 반환 컬럼이 예상과 다릅니다. missing={missing}, columns={list(out.columns)}")

    out = out[["time", "open", "high", "low", "close", "Volume"]].copy()
    out["time"] = pd.to_datetime(out["time"], errors="coerce")
    out = out.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

    for c in ["open", "high", "low", "close", "Volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out["date"] = out["time"].dt.strftime("%Y-%m-%d")
    return out


def _pick_day_row(df: pd.DataFrame, target_date: str) -> pd.Series:
    if df is None or df.empty:
        raise ValueError("TradingView에서 받은 데이터가 비어있습니다.")

    target_date = pd.to_datetime(target_date).strftime("%Y-%m-%d")
    hit = df[df["date"] == target_date]
    if hit.empty:
        raise ValueError(f"target_date={target_date}에 해당하는 일봉이 없습니다. (휴장/아직 미반영 가능)")
    return hit.iloc[-1]


def _load_prev_ema_from_history_exact(history_csv: str, prev_date: str) -> Optional[float]:
    """
    history_csv에서 'prev_date(D-2)'의 EMA를 정확히 찾아서 반환
    (주의: 이전 버전처럼 "< target_date 마지막"은 전날/전전날 헷갈릴 수 있어,
     여기서는 날짜 exact match로 잡음)
    """
    if not history_csv or not os.path.exists(history_csv):
        return None

    h = pd.read_csv(history_csv)

    # time 컬럼 이름 대응 (혹시 date/time 혼재)
    if "time" not in h.columns:
        return None
    if "EMA" not in h.columns:
        return None

    h["time"] = pd.to_datetime(h["time"], errors="coerce")
    h = h.dropna(subset=["time"]).sort_values("time")
    h["date"] = h["time"].dt.strftime("%Y-%m-%d")

    prev_date = pd.to_datetime(prev_date).strftime("%Y-%m-%d")
    hit = h[h["date"] == prev_date].tail(1)
    if hit.empty:
        return None

    try:
        v = float(hit["EMA"].iloc[0])
        return None if pd.isna(v) else v
    except Exception:
        return None


def _calc_ema_one(prev_ema: float, close_today: float, span: int) -> float:
    alpha = 2.0 / (span + 1.0)
    return alpha * close_today + (1.0 - alpha) * prev_ema


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="all", choices=["all", "corn", "soybean", "wheat"])
    ap.add_argument("--span", type=int, default=200)
    ap.add_argument("--n_bars", type=int, default=120, help="최근 n개 일봉(기본 120). 전날/전전날 포함되도록 넉넉히")

    ap.add_argument("--tv_user", default=os.getenv("TV_USER", None))
    ap.add_argument("--tv_pass", default=os.getenv("TV_PASS", None))
    ap.add_argument("--debug", action="store_true")

    # 어떤 날짜를 출력할지
    ap.add_argument("--mode", default="yday", choices=["yday", "latest"],
                    help="yday=전날(D-1) 출력(기본), latest=최신 일봉 출력")

    # prev_ema 소스
    ap.add_argument("--prev_ema", type=float, default=None,
                    help="전전날(D-2) EMA를 직접 주입(DB에서 읽어 넣을 때). target=all일 땐 사용 비권장")
    ap.add_argument("--history_csv", default=None,
                    help="전전날(D-2) EMA를 읽어올 CSV 경로(예: data/_new/corn_new_20251101_20260128.csv)")

    ap.add_argument("--out", default=None, help="저장 경로. 없으면 stdout에 JSON 출력")
    ap.add_argument("--format", default="json", choices=["json", "csv"])
    args = ap.parse_args()

    # 날짜 계산 (UTC 기준)
    today_utc = pd.Timestamp.utcnow().normalize()
    if args.mode == "yday":
        target_date = (today_utc - pd.Timedelta(days=1)).strftime("%Y-%m-%d")  # D-1
        prev_date = (today_utc - pd.Timedelta(days=2)).strftime("%Y-%m-%d")    # D-2
    else:
        # latest: TV 최신 일봉을 D-0로 보고, prev는 그 전날
        # (이 경우 date는 데이터에서 결정)
        target_date = None
        prev_date = None

    targets = ["corn", "soybean", "wheat"] if args.target == "all" else [args.target]
    results = []

    for com in targets:
        exchange, symbol = DEFAULT_SYMBOLS[com]
        df = _fetch_recent_bars(exchange, symbol, args.n_bars, args.tv_user, args.tv_pass, debug=args.debug)

        if df.empty:
            raise ValueError(f"{com}: TradingView 데이터가 비었습니다.")

        # target row 선택
        if args.mode == "latest":
            one = df.iloc[-1]
            target_date = one["date"]
            prev_date = df.iloc[-2]["date"] if len(df) >= 2 else None
            if prev_date is None:
                raise ValueError(f"{com}: prev_date를 정할 수 없습니다(n_bars를 늘려주세요).")
        else:
            one = _pick_day_row(df, target_date)

        close_target = float(one["close"])
        date_str = pd.to_datetime(one["time"]).strftime("%Y-%m-%d")

        # prev_ema(D-2) 확보
        prev_ema = None
        if args.prev_ema is not None and args.target != "all":
            prev_ema = float(args.prev_ema)
        else:
            # history_csv에서 prev_date(D-2) exact match로 찾음
            if prev_date is None:
                raise ValueError(f"{com}: prev_date가 None입니다.")
            prev_ema = _load_prev_ema_from_history_exact(args.history_csv, prev_date)

        if prev_ema is None:
            raise ValueError(
                f"{com}: 전전날(prev_date={prev_date}) EMA를 확보 못했습니다. "
                f"DB에서 prev_ema를 주입하거나(--prev_ema), history_csv를 올바르게 지정하세요."
            )

        ema_target = _calc_ema_one(prev_ema, close_target, args.span)

        # 출력 순서: time,open,high,low,close,EMA,Volume
        row = {
            "commodity": com,
            "time": date_str,
            "open": float(one["open"]),
            "high": float(one["high"]),
            "low": float(one["low"]),
            "close": close_target,
            "EMA": float(ema_target),
            "Volume": float(one["Volume"]) if not pd.isna(one["Volume"]) else None,
            "source": f"{exchange}:{symbol}",
            "span": args.span,
            "prev_date_used": prev_date,  # 디버깅용(원하면 빼도 됨)
        }
        results.append(row)

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        if args.format == "json":
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
        else:
            pd.DataFrame(results).to_csv(args.out, index=False)
        print(f"[save] {args.out}")
    else:
        print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
