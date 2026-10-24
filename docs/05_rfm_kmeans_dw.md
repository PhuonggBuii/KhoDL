# Phase 5 — RFM + K-Means tu Data Warehouse

Tai lieu nay mo ta pipeline RFM/K-Means MOI, doc lap voi baseline
(`scripts/03_rfm.py` + `scripts/04_kmeans.py`, input = `data/processed/
transactions_clean.csv`). Pipeline moi doc truc tiep tu PostgreSQL DW
(`fact_sales JOIN dim_customer`), dap ung dung yeu cau Phase 5.1 trong
`PROJECT_CHECKLIST.md` ("Dau vao: Fact_Sales + Dim_Customer da nap").

Baseline KHONG bi sua/xoa, van la ket qua da duoc nghiem thu truoc do va duoc
giu lam tham chieu doi chieu.

## 1. Cong thuc RFM va nguon du lieu DW

Nguon: `fact_sales f JOIN dim_customer c ON f.customer_key = c.customer_key`,
**chi lay dong co `f.customer_key IS NOT NULL`**.

Quy tac loai khach hang thieu Customer ID: ke thua dung quyet dinh da ghi trong
`docs/03_star_schema.md` muc 6 (quyet dinh #1 — fact_sales giu dong thieu
Customer ID voi `customer_key = NULL` de khong mat doanh thu, nhung loai khoi
RFM theo khach hang). Day la quy tac KY THUAT da duoc ap dung nhat quan trong
Phase 2.3-2.4 cua du an; tai lieu nay KHONG tuyen bo ca nhom da hop xac nhan
chinh thuc neu chua co bang chung ve viec do.

Cong thuc (giu nguyen y nghia baseline, chi doi nguon):

| Chi so | Cach tinh |
|---|---|
| Recency | So ngay tu lan mua gan nhat (`MAX(invoice_datetime)` theo khach hang) den ngay tham chieu |
| Frequency | `COUNT(DISTINCT invoice_no)` theo khach hang |
| Monetary | `SUM(revenue)` theo khach hang (`revenue` la cot GENERATED = `ROUND(quantity*unit_price,2)` trong `fact_sales`) |

Ngay tham chieu = `MAX(invoice_datetime)` **trong tap da loc** (`customer_key
IS NOT NULL`) **+ 1 ngay** — dung chinh xac cach tinh cua `scripts/03_rfm.py`,
vi input cua script do (`transactions_clean.csv`, tao boi `scripts/
02_clean_data.py`) da `dropna(subset=["Customer ID"])` TRUOC khi lay `.max()`.

Script: [`scripts/mining/rfm_dw.py`](../scripts/mining/rfm_dw.py). Ket noi DW
tai su dung dung pattern bien moi truong cua `scripts/load/load.py`
(`PG_DW_HOST`/`PG_DW_PORT_INTERNAL`, xem docstring file do).

## 2. Doi chieu voi baseline (da chay thuc te)

| | DW (`rfm_from_dw.csv`) | Baseline (`rfm.csv`) |
|---|---|---|
| So khach hang | 5.853 | 5.853 |
| Chi co trong 1 ben | 0 | 0 |
| Lech Recency (max) | 0.0000 | - |
| Lech Frequency (max) | 0.0000 | - |
| Lech Monetary (max) | 0.0020 (sai so dau phay dong, khong dang ke) | - |

=> RFM tinh tu DW **giong tuyet doi** baseline (nhu da du bao khi khao sat Phase
5 — xem bang chung spot-check khach hang 12346 truoc do). Dieu nay xac nhan: dù
2 pipeline doc 2 nguon khac nhau (CSV do `02_clean_data.py` tao vs DW do
`transform.py` + `load.py` tao), cung quy tac nghiep vu (bo trung, loai don
huy, loai Quantity/Price<=0, loai ma phi, loai thieu Customer ID khoi RFM) cho
ra ket qua nhat quan.

## 3. Tien xu ly va K-Means

Script: [`scripts/mining/cluster_dw.py`](../scripts/mining/cluster_dw.py).
Tien xu ly giu dung baseline: **khong** log-transform Recency; log1p(Frequency),
log1p(Monetary) (dung 1 lan, khong log1p 2 lan); `StandardScaler` tren ca 3 cot
sau buoc tren. `KMeans(random_state=42, n_init=10)` co dinh — ket qua tai lap
duoc.

### Bang Silhouette Score day du (K=2..10, tren du lieu DW)

| K | Silhouette |
|---|---|
| 2 | 0.4182 |
| 3 | 0.4017 |
| 4 | 0.3611 |
| 5 | 0.3652 |
| 6 | 0.3474 |
| 7 | 0.3357 |
| 8 | 0.3154 |
| 9 | 0.3116 |
| 10 | 0.3010 |

File day du: [`data/processed/silhouette_scores_from_dw.csv`](../data/processed/silhouette_scores_from_dw.csv).
Bieu do: [`docs/silhouette_scores_from_dw.png`](silhouette_scores_from_dw.png).

- `best_k` (Silhouette cao nhat, thuan thuat toan) = **2** (score=0.4182) — chi
  tach "con hoat dong" vs "khong hoat dong", it y nghia kinh doanh (giong nhan
  xet da co trong baseline).
- `business_k` (K tham khao de phan nhom co y nghia nghiep vu, ke thua quy tac
  tie-break cua baseline: `4 neu best_k != 4, nguoc lai 5`) = **4**
  (score=0.3611).
- **So sanh voi baseline**: Silhouette K=4 tren DW = **0.3611**, **giong tuyet
  doi** gia tri da bao cao cho baseline (0.3611, `docs/01_bao_cao_tien_do.md`).
  Khong lech — vi input RFM-tu-DW giong tuyet doi baseline (muc 2) va
  `random_state`/`n_init` giu nguyen, nen KMeans cho ket qua giong het, khong
  can gia dinh K=4 van hop ly — da duoc tinh lai va xac nhan bang so lieu thuc.

### Dac diem va dien giai 4 cum (business_k=4)

| Cluster | So KH | Recency_TB | Frequency_TB | Monetary_TB | Segment |
|---|---|---|---|---|---|
| 3 | 947 | 42.5 | 22.2 | 12.819,5 | Khach hang VIP |
| 0 | 1.875 | 98.3 | 5.5 | 1.871,3 | Khach hang trung thanh |
| 2 | 1.397 | 104.1 | 1.7 | 421,3 | Khach hang moi / it tuong tac |
| 1 | 1.634 | 490.8 | 1.7 | 517,0 | Nguy co roi bo |

Tong 947+1.875+1.397+1.634 = 5.853 — khop so khach hang da phan cum. Giong
tuyet doi profile da cong bo cho baseline (`docs/01_bao_cao_tien_do.md` muc 5).

**Quy tac gan `segment_name`** (heuristic xep hang tuong doi giua cac cum,
`scripts/mining/cluster_dw.py::_label_clusters`, KHONG phai chuan nganh):
1. Cum co `Recency_TB` cao nhat (lau khong mua lai nhat) -> "Nguy co roi bo",
   bat ke Frequency/Monetary.
2. Trong cac cum con lai, xep hang theo (hang Frequency_TB giam + hang
   Monetary_TB giam); hang tot nhat -> "Khach hang VIP", tiep theo -> "Khach
   hang trung thanh", con lai -> "Khach hang moi / it tuong tac".

Han che da biet: quy tac nay thiet ke cho K nho (video K=4); voi K lon hon,
nhieu cum co the cung roi vao "Khach hang moi / it tuong tac" (catch-all).
`cluster_label` (so nguyen tu KMeans) KHONG co thu tu co dinh giua cac lan
chay mo hinh khac nhau — luon dung `segment_name` de dien giai, khong dung
`cluster_label` truc tiep.

## 4. Thiet ke `customer_segment` va UPSERT

DDL: [`sql/003_customer_segment.sql`](../sql/003_customer_segment.sql).
Quyet dinh: bang RIENG (khong ALTER `dim_customer`) de khong thay doi Star
Schema da kiem thu o Phase 3/4.

| Cot | Kieu | Y nghia |
|---|---|---|
| `customer_key` | BIGINT, PK, FK -> `dim_customer(customer_key)` | 1 dong/khach hang da phan cum |
| `recency`, `frequency` | INTEGER | Tu `rfm_from_dw.csv` |
| `monetary` | NUMERIC(14,2) | Tu `rfm_from_dw.csv` |
| `k_used` | SMALLINT | So cum (business_k) dung de tao `cluster_label` nay |
| `cluster_label` | SMALLINT | Nhan cum tho tu KMeans (0..k_used-1) |
| `segment_name` | VARCHAR(50) | Ten doc hieu (muc 3) |
| `computed_at` | TIMESTAMP | Thoi diem UPSERT gan nhat |

Nap: [`scripts/mining/load_segments_dw.py`](../scripts/mining/load_segments_dw.py)
— `UPSERT` theo `customer_key` (`ON CONFLICT ... DO UPDATE`), KHONG TRUNCATE.
Da kiem thu chay 2 lan lien tiep: ca 2 lan deu cho **5.853 dong**, khong tang
do trung lap.

### Truy van mau cho Superset

```sql
SELECT segment_name, count(*) AS so_khach_hang,
       round(avg(recency),1) AS recency_tb,
       round(avg(frequency),1) AS frequency_tb,
       round(avg(monetary),1) AS monetary_tb
FROM customer_segment
GROUP BY segment_name
ORDER BY monetary_tb DESC;
```

```sql
SELECT cs.segment_name, dc.customer_id, cs.recency, cs.frequency, cs.monetary
FROM customer_segment cs
JOIN dim_customer dc ON cs.customer_key = dc.customer_key
ORDER BY cs.monetary DESC
LIMIT 20;
```

## 5. Lenh tai kiem thu

```bash
set -a && source .env && set +a   # (bash) hoac tuong duong tren PowerShell

uv run scripts/mining/rfm_dw.py              # -> data/processed/rfm_from_dw.csv, doi chieu baseline
uv run scripts/mining/cluster_dw.py          # -> customer_segments_from_dw.csv, silhouette_scores_from_dw.csv
docker exec -i khodl-postgres-dw psql -U "$PG_DW_USER" -d "$PG_DW_DB" -f - < sql/003_customer_segment.sql
uv run scripts/mining/load_segments_dw.py    # UPSERT - chay lai nhieu lan an toan

# Kiem tra:
docker exec khodl-postgres-dw psql -U "$PG_DW_USER" -d "$PG_DW_DB" -c \
  "SELECT count(*) FROM customer_segment;"                                        -- = 5853
docker exec khodl-postgres-dw psql -U "$PG_DW_USER" -d "$PG_DW_DB" -c \
  "SELECT count(*) FROM customer_segment cs LEFT JOIN dim_customer cu \
   ON cs.customer_key=cu.customer_key WHERE cu.customer_key IS NULL;"             -- = 0 (khong orphan)
docker exec khodl-postgres-dw psql -U "$PG_DW_USER" -d "$PG_DW_DB" -c \
  "SELECT segment_name, count(*) FROM customer_segment GROUP BY segment_name;"    -- tong = 5853
docker exec khodl-postgres-dw psql -U "$PG_DW_USER" -d "$PG_DW_DB" -c \
  "SELECT count(*), round(sum(revenue),2) FROM fact_sales;"                       -- = 1003670 / 19659453.86 (khong doi)
```

## 6. Gioi han da biet

- `segment_name` la heuristic xep hang tuong doi (muc 3), khong phai quy tac
  nghiep vu chinh thuc da duoc nhom phe duyet rieng cho ten nhan — can nhom xac
  nhan ten goi cuoi cung truoc khi dung trong bao cao chinh thuc.
- RFM-tu-DW ke thua toan bo cac quyet dinh nghiep vu cua Phase 2.3 (`scripts/
  transform/transform.py`), cac quyet dinh nay van o trang thai `[~]` (ky thuat
  da ap dung nhat quan, chua co bang chung nhom chinh thuc xac nhan) theo
  `PROJECT_CHECKLIST.md`.
- `cluster_label` khong on dinh giua cac lan chay lai mo hinh voi du lieu moi
  (vd. sau khi nap them giao dich) — `k_used` + `computed_at` cho phep nhan ra
  khi mo hinh da duoc chay lai.
