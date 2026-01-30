# 데이터 추가

EMA에 대한 정보가 나와있지 않아 역으로 계산해보는 코드를 짜서 실행해봤더니

span이 200인걸로 강하게 추정됨

2023.06.18에 제공받은 price 데이터셋의 volume들이 다 0임

⇒ 다른것도 오류가 있을지도?? 그냥 트레이딩뷰 api 이용해서 싹 가져와도 될지도 (아예새로만들기)

fetch_new_prices.py 파일 만들고

```jsx
YDAY=$(python -c "import pandas as pd; print((pd.Timestamp.utcnow().normalize()-pd.Timedelta(days=1)).strftime('%Y-%m-%d'))")

python fetch_new_prices.py \
  --data_dir ./data \
  --target all \
  --start 2025-11-01 \
  --end "$YDAY" \
  --n_bars 5000 \
  --add_ema \
  --ema_span 200

```

이거 실행시키면 2025년11월1일부터 어제 자정까지의 가격데이터 들어가게됨

이제 매일매일 업데이트 할떄에는

fetch_daily_tv.py 이거 파일 만들고

 

```jsx
python fetch_daily_tv.py --target corn    --mode yday --history_csv data/_new/corn_new_20251101_20260128.csv
python fetch_daily_tv.py --target soybean --mode yday --history_csv data/_new/soybean_new_20251101_20260128.csv
python fetch_daily_tv.py --target wheat   --mode yday --history_csv data/_new/wheat_new_20251101_20260128.csv

```

이거 실행시키면 작물별로 전날까지의 가격들과 ema를 가져옴

나중에 db연동하면

```jsx
python fetch_daily_tv.py --target corn --mode yday --prev_ema <DB에서 읽은 전전날 EMA>
```

이거로 작동하면 됨

# 뉴스데이터 추가

## (1) 단일 날짜 수집
```bash
python fetch_daily_news_gdelt.py \
  --date 2025-11-14 \
  --out ./data/_new/news_20251114_tilda_compat.csv \
  --max_per_keyword 300 \
  --dedupe_by_url 1 \
  --workers 12 \
  --sourcelang English \
  --verbose 1 \
  --log_every 50 \
  --print_fail 1
```

## (2) 기간 수집
```bash
python fetch_daily_news_gdelt.py \
  --date_from 2025-11-01 \
  --date_to 2025-11-14 \
  --out ./data/_new/news_20251101_1114_tilda_compat.csv \
  --max_per_keyword 300 \
  --dedupe_by_url 1 \
  --workers 12 \
  --sourcelang English \
  --verbose 1
```
## 주요 옵션 설명
옵션	의미

`--date`	단일 날짜(YYYY-MM-DD)

`--date_from`, `--date_to`	기간 수집

`--out`	출력 CSV 경로

`--max_per_keyword`	키워드당 최대 기사 수(0이면 제한 없음)

`--dedupe_by_url`	1이면 URL 기준 중복 제거

`--workers`	기사 페이지 크롤링 병렬 스레드 수

`--sourcelang`	GDELT 언어 필터 (예: English, ALL)

`--all_text_maxlen`	all_text 최대 길이(기본 215)

`--description_maxlen`	description 최대 길이(기본 260)

`--max_total_candidates`	(가드레일) 전체 후보 URL 상한
--max_pages_per_keyword	(가드레일) 키워드별 ArtList 페이지 상한
--max_runtime_sec	(가드레일) 전체 실행 시간 상한
--print_fail	1이면 실패/재시도 로그 출력
--verbose	1이면 진행 로그 출력
