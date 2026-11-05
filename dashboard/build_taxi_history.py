# -*- coding: utf-8 -*-
"""
从 NYC TLC 公开历史出租车行程数据聚合出仿真标定参数。

数据源：NYC TLC Yellow Taxi Trip Records, 2024-01
        https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
许可：Public Domain（纽约市开放数据）

用法：
    python build_taxi_history.py
输出：
    <repo>/.taxi_data/taxi_history.json
"""
import json
import os
import sys

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)          # mobilitypilot/
DATA = os.path.join(ROOT, '.taxi_data')
PARQUET = os.path.join(DATA, 'yellow_tripdata_2024-01.parquet')
ZONES = os.path.join(DATA, 'taxi_zone_lookup.csv')
OUT = os.path.join(DATA, 'taxi_history.json')

BASE = 'https://d37ci6vzurychx.cloudfront.net'
PARQUET_URL = BASE + '/trip-data/yellow_tripdata_2024-01.parquet'
ZONES_URL = BASE + '/misc/taxi_zone_lookup.csv'

MI_TO_KM = 1.609344


def fetch(url, dst):
    """缺失时自动下载（首次运行需要约 50 MB）。"""
    import urllib.request
    os.makedirs(DATA, exist_ok=True)
    print('下载', url)
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=180) as r, open(dst, 'wb') as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
    print('  ->', dst, f'({os.path.getsize(dst)/1048576:.1f} MB)')


if not os.path.exists(PARQUET):
    fetch(PARQUET_URL, PARQUET)
if not os.path.exists(ZONES):
    fetch(ZONES_URL, ZONES)

con = duckdb.connect()
con.execute(f"CREATE VIEW raw AS SELECT * FROM read_parquet('{PARQUET}')")

rows_raw = con.execute("SELECT count(*) FROM raw").fetchone()[0]

# ---------- 清洗 ----------
CLEAN = """
CREATE VIEW trips AS
SELECT
  tpep_pickup_datetime                                   AS pu,
  trip_distance * {k}                                    AS km,
  date_diff('second', tpep_pickup_datetime,
                     tpep_dropoff_datetime) / 60.0       AS mins,
  trip_distance / (date_diff('second', tpep_pickup_datetime,
                     tpep_dropoff_datetime) / 3600.0) * {k} AS kmh,
  passenger_count                                        AS pax,
  payment_type                                           AS pay,
  PULocationID                                           AS puz,
  fare_amount, tip_amount, tolls_amount, total_amount,
  hour(tpep_pickup_datetime)                             AS hh,
  (dayofweek(tpep_pickup_datetime) + 6) % 7              AS dow   -- 0=周一
FROM raw
WHERE tpep_pickup_datetime >= TIMESTAMP '2024-01-01'
  AND tpep_pickup_datetime <  TIMESTAMP '2024-02-01'
  AND trip_distance > 0 AND trip_distance <= 100
  AND date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) BETWEEN 60 AND 10800
  AND fare_amount > 0 AND fare_amount < 500
  AND total_amount > 0
  AND passenger_count BETWEEN 1 AND 6
  AND trip_distance / (date_diff('second', tpep_pickup_datetime,
        tpep_dropoff_datetime) / 3600.0) * {k} BETWEEN 1 AND 110
""".format(k=MI_TO_KM)
con.execute(CLEAN)

rows_clean = con.execute("SELECT count(*) FROM trips").fetchone()[0]


def q(sql):
    return con.execute(sql).fetchall()


def r2(x):
    return None if x is None else round(float(x), 2)


# ---------- 总体指标 ----------
k = q("""SELECT count(*), avg(km), median(km), quantile_cont(km, 0.9),
                avg(mins), median(mins), avg(kmh), median(kmh),
                avg(total_amount), median(total_amount),
                sum(tip_amount) / nullif(sum(total_amount), 0),
                avg(pax), avg(fare_amount)
         FROM trips""")[0]
kpi = {
    'trips': rows_clean,
    'trips_per_day': r2(rows_clean / 31.0),
    'avg_dist_km': r2(k[1]), 'median_dist_km': r2(k[2]), 'p90_dist_km': r2(k[3]),
    'avg_dur_min': r2(k[4]), 'median_dur_min': r2(k[5]),
    'avg_speed_kmh': r2(k[6]), 'median_speed_kmh': r2(k[7]),
    'avg_total_usd': r2(k[8]), 'median_total_usd': r2(k[9]),
    'tip_ratio': r2(k[10] * 100),
    'avg_pax': r2(k[11]), 'avg_fare_usd': r2(k[12]),
}

# ---------- 逐小时 ----------
hourly = []
for hh, n, sp, dm in q("""SELECT hh, count(*), avg(kmh), avg(km)
                          FROM trips GROUP BY hh ORDER BY hh"""):
    hourly.append({'h': int(hh), 'trips': int(n),
                   'share': r2(n * 100.0 / rows_clean),
                   'speed': r2(sp), 'dist': r2(dm)})

# ---------- 星期 × 小时 ----------
grid = [[0] * 24 for _ in range(7)]
for dow, hh, n in q("SELECT dow, hh, count(*) FROM trips GROUP BY dow, hh"):
    grid[int(dow)][int(hh)] = int(n)
peak = max(max(r) for r in grid)
wd_hour = [[r2(v * 100.0 / peak) for v in row] for row in grid]

# ---------- 距离分箱 ----------
DIST_EDGES = [(0, 1), (1, 2), (2, 3), (3, 5), (5, 8), (8, 12), (12, 20), (20, 999)]
dist_bins = []
for a, b in DIST_EDGES:
    n = q(f"SELECT count(*) FROM trips WHERE km >= {a} AND km < {b}")[0][0]
    label = f'{a}-{b}' if b != 999 else f'{a}+'
    dist_bins.append({'label': label, 'share': r2(n * 100.0 / rows_clean), 'n': int(n)})

# ---------- 时长分箱 ----------
DUR_EDGES = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 30), (30, 45), (45, 60), (60, 999)]
dur_bins = []
for a, b in DUR_EDGES:
    n = q(f"SELECT count(*) FROM trips WHERE mins >= {a} AND mins < {b}")[0][0]
    label = f'{a}-{b}' if b != 999 else f'{a}+'
    dur_bins.append({'label': label, 'share': r2(n * 100.0 / rows_clean), 'n': int(n)})

# ---------- 支付方式 ----------
PAY_NAME = {0: '未知', 1: '信用卡', 2: '现金', 3: '免单', 4: '争议', 5: '未知', 6: '作废'}
pay = []
for p, n in q("SELECT pay, count(*) FROM trips GROUP BY pay ORDER BY count(*) DESC"):
    pay.append({'label': PAY_NAME.get(int(p), f'类型{int(p)}'),
                'share': r2(n * 100.0 / rows_clean)})

# ---------- 载客人数 ----------
pax = []
for p, n in q("SELECT pax, count(*) FROM trips GROUP BY pax ORDER BY pax"):
    pax.append({'n': int(p), 'share': r2(n * 100.0 / rows_clean)})

# ---------- 热门上车区 ----------
zone_name = {}
if os.path.exists(ZONES):
    import csv
    with open(ZONES, encoding='utf-8-sig', newline='') as f:
        for row in csv.DictReader(f):
            try:
                zone_name[int(row['LocationID'])] = row['Zone']
            except (KeyError, ValueError):
                pass

top_zones = []
for zid, n in q("SELECT puz, count(*) c FROM trips GROUP BY puz ORDER BY c DESC LIMIT 8"):
    top_zones.append({'id': int(zid), 'name': zone_name.get(int(zid), f'Zone {zid}'),
                      'share': r2(n * 100.0 / rows_clean)})

# ---------- 工作日 / 周末 ----------
wk = q("SELECT count(*) FROM trips WHERE dow <= 4")[0][0]
we = q("SELECT count(*) FROM trips WHERE dow >= 5")[0][0]
# 2024-01 有 23 个工作日、8 个周末日
split = {'weekday_per_day': r2(wk / 23.0), 'weekend_per_day': r2(we / 8.0)}

out = {
    'source': {
        'name': 'NYC TLC Yellow Taxi Trip Records',
        'period': '2024-01',
        'page': 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page',
        'file': 'yellow_tripdata_2024-01.parquet',
        'license': 'Public Domain (NYC Open Data)',
        'rows_raw': int(rows_raw),
        'rows_clean': int(rows_clean),
        'keep_ratio': r2(rows_clean * 100.0 / rows_raw),
        'days': 31,
        'unit_note': '原始单位为英里，已按 1 mile = 1.609344 km 换算',
    },
    'kpi': kpi,
    'hourly': hourly,
    'weekday_hour': wd_hour,
    'dist_bins': dist_bins,
    'dur_bins': dur_bins,
    'pay': pay,
    'pax': pax,
    'top_zones': top_zones,
    'split': split,
}

os.makedirs(DATA, exist_ok=True)
with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(out, f, ensure_ascii=False, separators=(',', ':'))

print(f'原始 {rows_raw:,} 行 -> 清洗后 {rows_clean:,} 行 ({out["source"]["keep_ratio"]}%)')
print(f'日均 {kpi["trips_per_day"]:,.0f} 单 | 均距 {kpi["avg_dist_km"]} km | '
      f'均速 {kpi["avg_speed_kmh"]} km/h | 单均 ${kpi["avg_total_usd"]}')
print(f'高峰时段 {max(hourly, key=lambda x: x["trips"])["h"]}:00 '
      f'({max(x["trips"] for x in hourly):,} 单)')
print('Top 上车区:', ', '.join(f'{z["name"]} {z["share"]}%' for z in top_zones[:4]))
print('输出 ->', OUT, f'({os.path.getsize(OUT)/1024:.1f} KB)')
