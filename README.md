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
