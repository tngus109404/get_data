#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fetch_new_prices.py (TradingView version)

기존 CSV를 건드리지 않고, 'start ~ end' 구간의 가격 데이터를 TradingView에서 받아 저장합니다.

TradingView 심볼:
  corn   : CBOT:ZC1!
  soybean: CBOT:ZM1!
  wheat  : CBOT:ZW1!

필요:
  pip install tradingview-datafeed pandas

중요:
- tvDatafeed의 get_hist는 start/end로 딱 맞춰 가져오는 게 아니라 "최근 n_bars개"를 가져옵니다.
- 과거 구간(예: 2025-10~2025-11)을 커버하려면 n_bars를 충분히 크게 잡아야 합니다.

EMA 관련(핵심 변경점):
- EMA 비교를 "제공받은 CSV와 동일하게" 하려면,
  TradingView에서 구간 가격을 받아온 뒤, EMA는 기존 CSV의 직전 EMA를 seed로 해서
  start~end 구간만 재귀식(=adjust=False)으로 계산하는 게 가장 안전합니다.
"""

import argparse
import os
import sys
import time
import logging
import pandas as pd

# ✅ tvDatafeed 로거 소음 제거
for name in ["tvDatafeed", "tvDatafeed.main", "tvdatafeed", "tvdatafeed.main"]:
    logging.getLogger(name).setLevel(logging.CRITICAL)

DEFAULT_FILENAMES = {
    "corn": "corn_future_price.csv",
    "soybean": "soybean_future_price.csv",
    "wheat": "wheat_future_price.csv",
}

DEFAULT_SYMBOLS = {
    "corn": "ZC1!",
    "soybean": "ZM1!",
    "wheat": "ZW1!",
}

DEFAULT_EXCHANGES = {
    "corn": "CBOT",
    "soybean": "CBOT",
    "wheat": "CBOT",
}

NEED_BASE = ["time", "open", "high", "low", "close", "Volume"]
TIME_CANDIDATES = ["time", "Date", "date", "Datetime", "datetime", "timestamp", "index", "level_0"]


def _ensure_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def _read_existing_last_date(csv_path: str) -> pd.Timestamp:
    df = pd.read_csv(csv_path)

    # Unnamed 제거
    for c in list(df.columns):
        if str(c).lower().startswith("unnamed"):
            df = df.drop(columns=[c])

    time_col = None
    for cand in TIME_CANDIDATES:
        if cand in df.columns:
            time_col = cand
            break
    if time_col is None:
        raise ValueError(f"기존 CSV에서 time/date 컬럼을 찾지 못했습니다: {csv_path}")

    dt = _ensure_datetime(df[time_col]).dropna()
    if dt.empty:
        raise ValueError(f"기존 CSV에서 유효한 datetime이 없습니다: {csv_path}")
    return dt.max()


def _import_tvdatafeed():
    """환경에 따라 import 경로가 tvDatafeed / tvdatafeed 로 다를 수 있어 둘 다 시도."""
    try:
        from tvDatafeed import TvDatafeed, Interval
        return TvDatafeed, Interval
    except Exception:
        from tvdatafeed import TvDatafeed, Interval
        return TvDatafeed, Interval


def _estimate_n_bars(start: str, end: str, cap: int = 5000) -> int:
    """
    tvDatafeed가 "최근 n개"를 가져오므로, 오늘 기준으로 start까지 커버되도록 n_bars를 대충 추정.
    """
    try:
        s = pd.to_datetime(start)
    except Exception:
        return 500

    today = pd.Timestamp.today().normalize()
    cal_days = int((today - s).days)
    if cal_days < 0:
        cal_days = 0

    trading_days = int(cal_days * 5 / 7)
    n_bars = trading_days + 120  # 버퍼
    n_bars = max(200, n_bars)
    n_bars = min(cap, n_bars)
    return n_bars


def _download_tradingview(
    symbol: str,
    exchange: str,
    start: str,
    end: str,
    tv_user: str | None = None,
    tv_pass: str | None = None,
    debug: bool = False,
    retries: int = 5,
    retry_sleep_base: int = 2,
    n_bars: int | None = None,
    n_bars_cap: int = 5000,
) -> pd.DataFrame:
    TvDatafeed, Interval = _import_tvdatafeed()
    tv = TvDatafeed(tv_user, tv_pass)

    if n_bars is None:
        n_bars = _estimate_n_bars(start, end, cap=n_bars_cap)

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
        time.sleep(retry_sleep_base * (i + 1))

    if df is None or df.empty:
        if last_err is not None:
            print(f"[WARN] TradingView fetch failed after retries({retries}): {last_err}", file=sys.stderr)
        return pd.DataFrame(columns=NEED_BASE)

    if debug:
        print("[DEBUG] tv raw columns:", list(df.columns))
        print("[DEBUG] tv raw index name:", df.index.name)
        print("[DEBUG] n_bars used:", n_bars)
        print("[DEBUG] tv raw head:\n", df.head(3))

    out = df.copy().reset_index()

    # time 컬럼명 통일
    if "datetime" in out.columns:
        out = out.rename(columns={"datetime": "time"})
    elif "date" in out.columns:
        out = out.rename(columns={"date": "time"})
    elif "time" not in out.columns:
        out = out.rename(columns={out.columns[0]: "time"})

    # volume -> Volume
    if "volume" in out.columns and "Volume" not in out.columns:
        out = out.rename(columns={"volume": "Volume"})

    missing = [c for c in NEED_BASE if c not in out.columns]
    if missing:
        raise ValueError(f"TradingView 데이터 컬럼이 예상과 다릅니다. missing={missing}, columns={list(out.columns)}")

    out = out[NEED_BASE].copy()
    out["time"] = pd.to_datetime(out["time"], errors="coerce")
    out = out.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

    for c in ["open", "high", "low", "close"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce")

    # end 포함 안정화: end+1day 버퍼 후 '<'
    s = pd.to_datetime(start)
    e = pd.to_datetime(end) + pd.Timedelta(days=1)
    out = out[(out["time"] >= s) & (out["time"] < e)].reset_index(drop=True)

    return out


def _attach_ema_seed_from_old_csv(old_csv_path: str, df_new: pd.DataFrame, span: int) -> pd.DataFrame:
    """
    ✅ 제공받은 CSV와 "같은 방식(연속성)"으로 EMA를 만들기 위해:
    - old_csv에서 start 이전 마지막 EMA(없으면 old close로 재계산)를 seed로 잡고
    - df_new 구간에 대해서만 EMA(adjust=False 재귀식) 계산
    """
    if df_new is None or df_new.empty:
        df_new = pd.DataFrame(columns=NEED_BASE + ["EMA"])
        return df_new

    old = pd.read_csv(old_csv_path)
    if "time" not in old.columns:
        # TIME_CANDIDATES 중 하나 찾기
        tcol = None
        for cand in TIME_CANDIDATES:
            if cand in old.columns:
                tcol = cand
                break
        if tcol is None:
            raise ValueError(f"old csv에 time 컬럼이 없습니다: {old_csv_path}")
        old = old.rename(columns={tcol: "time"})

    old["time"] = pd.to_datetime(old["time"], errors="coerce").dt.normalize()
    old = old.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

    new = df_new.copy()
    new["time"] = pd.to_datetime(new["time"], errors="coerce").dt.normalize()
    new = new.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

    start_dt = new["time"].iloc[0]
    prefix = old[old["time"] < start_dt].copy()

    # seed EMA 구하기
    seed_ema = None
    if not prefix.empty and ("EMA" in prefix.columns):
        # 마지막 EMA가 NaN일 수도 있으니 역방향으로 찾아보기
        ema_series = pd.to_numeric(prefix["EMA"], errors="coerce")
        idx = ema_series.last_valid_index()
        if idx is not None:
            seed_ema = float(ema_series.loc[idx])

    if seed_ema is None:
        # old에 EMA가 없으면 old close로 EMA를 한 번 재계산해서 seed를 만든다
        if prefix.empty:
            seed_ema = float(pd.to_numeric(new["close"].iloc[0], errors="coerce"))
        else:
            closes_prefix = pd.to_numeric(prefix["close"], errors="coerce")
            closes_prefix = closes_prefix.dropna().tolist()
            if not closes_prefix:
                seed_ema = float(pd.to_numeric(new["close"].iloc[0], errors="coerce"))
            else:
                alpha = 2.0 / (span + 1.0)
                ema_prev = float(closes_prefix[0])
                for c in closes_prefix:
                    ema_prev = alpha * float(c) + (1.0 - alpha) * ema_prev
                seed_ema = ema_prev

    # new 구간 EMA(adjust=False) 재귀식
    alpha = 2.0 / (span + 1.0)
    ema_prev = float(seed_ema)

    closes_new = pd.to_numeric(new["close"], errors="coerce").tolist()
    emas = []
    for c in closes_new:
        if c is None or (isinstance(c, float) and pd.isna(c)):
            emas.append(float("nan"))
            continue
        ema_prev = alpha * float(c) + (1.0 - alpha) * ema_prev
        emas.append(ema_prev)

    new["EMA"] = emas

    # 원래 df_new 컬럼 순서 유지 + EMA 추가
    out = df_new.copy()
    out["time"] = pd.to_datetime(out["time"], errors="coerce").dt.normalize()
    out = out.merge(new[["time", "EMA"]], on="time", how="left")
    return out


def _qc_report(df_new: pd.DataFrame, commodity: str):
    print("\n" + "-" * 90)
    print(f"[QC] {commodity}")
    print("-" * 90)

    if df_new is None or df_new.empty:
        print("신규 데이터가 비어 있습니다(다운로드 결과 없음).")
        return

    tmin, tmax = df_new["time"].min(), df_new["time"].max()
    print(f"rows={len(df_new)}  range={pd.to_datetime(tmin).date()} ~ {pd.to_datetime(tmax).date()}")

    dup = int(df_new.duplicated(subset=["time"]).sum())
    mono = bool(pd.to_datetime(df_new["time"]).is_monotonic_increasing)
    print(f"duplicates(time)={dup}  monotonic_increasing={mono}")

    nulls = df_new.isna().sum()
    if int(nulls.sum()) > 0:
        print("[null counts]")
        print(nulls.to_string())

    close = pd.to_numeric(df_new["close"], errors="coerce").astype(float)
    ret = close.pct_change()
    big = df_new.loc[ret.abs() > 0.2, ["time", "open", "high", "low", "close"]].copy()
    if not big.empty:
        print("\n[WARN] close 일간 변동률 |ret| > 20% 발견(확인 권장)")
        big["ret_%"] = (ret[ret.abs() > 0.2] * 100).round(2).values
        print(big.tail(10).to_string(index=False))
    else:
        print("close 일간 변동률 |ret| > 20% 없음")

    vol = pd.to_numeric(df_new["Volume"], errors="coerce")
    n0 = int((vol.fillna(0) == 0).sum())
    small = df_new.loc[vol.fillna(0) < 1000, ["time", "Volume", "close"]].copy()
    print(f"Volume==0 count={n0}")
    if not small.empty:
        print("\n[NOTE] Volume < 1000 행(휴장/집계 이슈일 수 있음) - 상위 10개")
        print(small.head(10).to_string(index=False))


def _save_new(df_new: pd.DataFrame, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if df_new is None or df_new.empty:
        # empty라도 columns 유지해서 저장
        cols = ["time","open","high","low","close"] + (["EMA"] if "EMA" in df_new.columns else []) + ["Volume"]
        pd.DataFrame(columns=cols).to_csv(out_path, index=False)
        return

    df = df_new.copy()
    df["time"] = pd.to_datetime(df["time"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").astype("Int64")

    # ✅ 제공받은 데이터셋 순서: time,open,high,low,close,EMA,Volume
    cols = ["time","open","high","low","close"] + (["EMA"] if "EMA" in df.columns else []) + ["Volume"]
    df.to_csv(out_path, index=False, columns=cols)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", default="./data")
    ap.add_argument("--out_dir", default=None, help="기본: <data_dir>/_new")
    ap.add_argument("--target", default="all", choices=["all", "corn", "soybean", "wheat"])
    ap.add_argument("--start", default=None, help="YYYY-MM-DD. 없으면 기존 max(time)+1")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD. 기본=오늘")

    ap.add_argument("--tv_user", default=os.getenv("TV_USER", None))
    ap.add_argument("--tv_pass", default=os.getenv("TV_PASS", None))
    ap.add_argument("--debug", action="store_true", help="TradingView raw head/columns 출력")

    ap.add_argument("--n_bars", type=int, default=None,
                    help="TradingView에서 가져올 일봉 개수(최근 n개). 과거 검증이면 2000~5000 권장.")
    ap.add_argument("--n_bars_cap", type=int, default=5000,
                    help="자동 추정 시 n_bars 최대치(기본 5000).")

    # ✅ EMA 옵션: 제공받은 CSV seed 방식(=adjust=False 연속 EMA)
    ap.add_argument("--add_ema", action="store_true", help="저장 CSV에 EMA 컬럼 포함(기존 CSV seed 방식)")
    ap.add_argument("--ema_span", type=int, default=200, help="EMA span (기본 200)")
    ap.add_argument("--ema_adjust", action="store_true",
                    help="(지원 안 함) seed 방식은 adjust=False만 지원. 켜면 에러.")

    ap.add_argument("--corn_symbol", default=DEFAULT_SYMBOLS["corn"])
    ap.add_argument("--soybean_symbol", default=DEFAULT_SYMBOLS["soybean"])
    ap.add_argument("--wheat_symbol", default=DEFAULT_SYMBOLS["wheat"])
    ap.add_argument("--corn_exchange", default=DEFAULT_EXCHANGES["corn"])
    ap.add_argument("--soybean_exchange", default=DEFAULT_EXCHANGES["soybean"])
    ap.add_argument("--wheat_exchange", default=DEFAULT_EXCHANGES["wheat"])

    args = ap.parse_args()

    if args.ema_adjust:
        raise ValueError("seed 방식 EMA는 adjust=False만 지원합니다. --ema_adjust 옵션을 제거하세요.")

    end = args.end or pd.Timestamp.today().strftime("%Y-%m-%d")
    out_dir = args.out_dir or os.path.join(args.data_dir, "_new")

    targets = ["corn", "soybean", "wheat"] if args.target == "all" else [args.target]
    symbols = {"corn": args.corn_symbol, "soybean": args.soybean_symbol, "wheat": args.wheat_symbol}
    exchanges = {"corn": args.corn_exchange, "soybean": args.soybean_exchange, "wheat": args.wheat_exchange}

    for com in targets:
        csv_path = os.path.join(args.data_dir, DEFAULT_FILENAMES[com])
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"기존 CSV가 없습니다: {csv_path}")

        if args.start:
            start = args.start
            last_dt = None
        else:
            last_dt = _read_existing_last_date(csv_path)
            start = (last_dt + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        print("\n" + "=" * 90)
        print(f"[FETCH] {com}  symbol={exchanges[com]}:{symbols[com]}")
        print("=" * 90)
        if last_dt is not None:
            print(f"existing_last_date={last_dt.date()}  -> start={start}  end={end}")
        else:
            print(f"start={start}  end={end}")

        df_new = _download_tradingview(
            symbol=symbols[com],
            exchange=exchanges[com],
            start=start,
            end=end,
            tv_user=args.tv_user,
            tv_pass=args.tv_pass,
            debug=args.debug,
            n_bars=args.n_bars,
            n_bars_cap=args.n_bars_cap,
        )

        # ✅ EMA는 기존 CSV seed 방식으로 start~end 구간만 계산
        if args.add_ema:
            df_new = _attach_ema_seed_from_old_csv(csv_path, df_new, span=int(args.ema_span))

        _qc_report(df_new, commodity=com)

        start_tag = start.replace("-", "")
        end_tag = end.replace("-", "")
        out_path = os.path.join(out_dir, f"{com}_new_{start_tag}_{end_tag}.csv")
        _save_new(df_new, out_path)
        print(f"\n[save] {out_path}")

    print("\n[DONE] 기존 CSV는 수정하지 않았습니다.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
