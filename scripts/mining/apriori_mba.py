"""Phase 8.1 - Market Basket Analysis (luat ket hop Apriori) tren Online Retail II.

Doc lap voi pipeline chinh (ETL/Star Schema/RFM/K-Means/Superset) - KHONG sua bat
ky file nao cua cac Phase truoc. Module nay chi THEM moi.

Bieu dien giao dich (transaction): 1 hoa don (Invoice) = 1 "gio hang" (basket),
item = ma san pham (StockCode). Dung dung quy tac lam sach da thong nhat cua du
an (xem docs/03_star_schema.md muc 6, PROJECT_CONTEXT.md muc 5):
  - Loai don huy (Invoice bat dau bang 'C').
  - Loai Quantity/Price <= 0 va ma phi/dieu chinh so sach (POST, DOT, D, M...).
  - KHONG loai dong thieu Customer ID - khac RFM (5.1), MBA phan tich san pham
    mua cung nhau trong 1 hoa don, KHONG can biet danh tinh khach hang.

Nguon du lieu (2 lua chon, --source):
  - "dw" (mac dinh, dung kien truc chinh thuc): doc truc tiep fact_sales JOIN
    dim_product tu PostgreSQL DW - fact_sales DA duoc Load (2.4) ap dung du 5
    quy tac lam sach, khong can loc lai o day. Giong pattern ket noi cua
    scripts/mining/rfm_dw.py.
  - "csv" (du phong / phat trien doc lap, khong can Docker dang chay): doc
    datalake/processed/<ngay>/{fact_sales,dim_product}.csv - DAY LA DUNG DU
    LIEU DA QUA Transform (Phase 2.3), tuc la CUNG mot noi dung da duoc Load
    vao DW (chi khac: khong phai query qua Postgres song). Dung khi can tai
    lap ket qua ma khong co docker compose dang chay (xem README muc "Chay".

Thuat toan Apriori tu cai dat (KHONG dung mlxtend/efficient-apriori/apyori -
du an chua co thu vien tuong duong nao, va cach lam nay TRANH one-hot encoding
toan bo ma tran giao dich x san pham (~39.536 hoa don x ~4.905 san pham neu
lam dense se ~194 trieu o) - chi dem itemset tren tung "gio hang" dang
frozenset, bo nho ti le voi so (hoa don, san pham) THUC SU xuat hien (~1 trieu
dong), khong phai ma tran day dac.

Chi so: support = P(itemset xuat hien trong hoa don), confidence(A->B) =
support(A u B) / support(A), lift(A->B) = confidence(A->B) / support(B).
lift > 1: B xuat hien cung A nhieu hon ngau nhien (lien he duong). lift = 1:
doc lap. lift < 1: lien he am (mua A giam kha nang mua B). KHONG suy dien
quan he nhan qua tu cac chi so nay (chi la dong xuat hien thong ke).

Chay (xem huong dan day du trong docstring cuoi file / docs/08_apriori_mba.md):
  uv run scripts/mining/apriori_mba.py                       # mac dinh --source dw
  uv run scripts/mining/apriori_mba.py --source csv          # khong can Docker
  uv run scripts/mining/apriori_mba.py --min-support 0.015 --min-confidence 0.25 --max-len 3
"""

from __future__ import annotations

import argparse
import itertools
import os
import sys
import time
from collections import Counter
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from common import PROCESSED_DIR as DATALAKE_PROCESSED_DIR  # noqa: E402
from common import ensure_dirs, load_env_file, log_ingest  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
LOCAL_PROCESSED_DIR = ROOT_DIR / "data" / "processed"
ITEMSETS_OUTPUT_PATH = LOCAL_PROCESSED_DIR / "apriori_frequent_itemsets.csv"
RULES_OUTPUT_PATH = LOCAL_PROCESSED_DIR / "apriori_association_rules.csv"

# Nguong mac dinh - giai thich trong docstring run()/README, dua tren khao sat
# thuc te phan phoi support cua 4.905 san pham tren 39.536 hoa don (xem
# docs/08_apriori_mba.md muc 2): min_support=0.02 (~791 hoa don) cho ~265
# san pham "pho bien" (du nho de liet ke, du lon de co y nghia nghiep vu);
# min_confidence=0.3 la nguong dien hinh cho MBA ban le (it nhat 30% hoa don
# co A thi cung co B).
DEFAULT_MIN_SUPPORT = 0.02
DEFAULT_MIN_CONFIDENCE = 0.30
DEFAULT_MAX_LEN = 2

DW_QUERY = """
    SELECT f.invoice_no AS invoice_no,
           p.stock_code AS stock_code
    FROM fact_sales f
    JOIN dim_product p ON f.product_key = p.product_key
"""


# =============================================================================
# 1. Doc + xac thuc du lieu dau vao
# =============================================================================

def validate_transactions(df: pd.DataFrame) -> None:
    """Kiem tra du lieu dau vao da duoc lam sach dung quy tac cua du an.

    KHONG tu lam sach lai o day (khac trach nhiem) - neu phat hien vi pham,
    bao loi ro rang thay vi am tham loc bo, de nguoi dung biet nguon du lieu
    chua qua Transform (2.3)/Load (2.4) dung cach.
    """
    required_cols = {"invoice_no", "stock_code"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Thieu cot bat buoc trong du lieu dau vao: {sorted(missing)}")

    if df.empty:
        raise ValueError("Du lieu dau vao rong - khong co giao dich nao de phan tich Apriori.")

    cancelled = df["invoice_no"].astype(str).str.startswith("C")
    if cancelled.any():
        raise ValueError(
            f"Phat hien {int(cancelled.sum()):,} dong hoa don huy (Invoice bat dau bang 'C') "
            "trong du lieu dau vao - fact_sales/datalake processed phai da loai don huy "
            "theo quy tac 0.3/Transform (2.3). Kiem tra lai nguon du lieu truoc khi chay Apriori."
        )

    if "quantity" in df.columns:
        n_bad_qty = int((df["quantity"] <= 0).sum())
        if n_bad_qty:
            raise ValueError(
                f"Phat hien {n_bad_qty:,} dong Quantity <= 0 - du lieu dau vao phai duoc "
                "lam sach truoc (quy tac 0.3/Transform 2.3)."
            )

    if df["stock_code"].isna().any() or df["invoice_no"].isna().any():
        raise ValueError("Phat hien gia tri NULL o invoice_no/stock_code - du lieu dau vao khong hop le.")


def _connection_params() -> dict:
    # Giong het scripts/mining/rfm_dw.py / scripts/load/load.py: PG_DW_PORT
    # (.env) la port PUBLISH ra host; qua ten service noi bo phai dung
    # PG_DW_PORT_INTERNAL (mac dinh = PG_DW_PORT khi chay tren host).
    return {
        "host": os.environ.get("PG_DW_HOST", "localhost"),
        "port": os.environ.get("PG_DW_PORT_INTERNAL", os.environ["PG_DW_PORT"]),
        "dbname": os.environ["PG_DW_DB"],
        "user": os.environ["PG_DW_USER"],
        "password": os.environ["PG_DW_PASSWORD"],
    }


def load_transactions_from_dw() -> pd.DataFrame:
    import psycopg2  # import cuc bo - chi can khi dung --source dw

    conn = psycopg2.connect(**_connection_params())
    try:
        with conn.cursor() as cur:
            cur.execute(DW_QUERY)
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


def _latest_datalake_processed_dir() -> Path | None:
    if not DATALAKE_PROCESSED_DIR.exists():
        return None
    candidates = sorted(
        (d for d in DATALAKE_PROCESSED_DIR.iterdir() if d.is_dir() and (d / "fact_sales.csv").exists()),
        reverse=True,
    )
    return candidates[0] if candidates else None


def load_transactions_from_csv(input_dir: Path | None) -> pd.DataFrame:
    resolved_dir = input_dir or _latest_datalake_processed_dir()
    if resolved_dir is None:
        raise FileNotFoundError(
            f"Khong tim thay thu muc nao co fact_sales.csv trong {DATALAKE_PROCESSED_DIR}. "
            "Chay scripts/transform/transform.py truoc, hoac truyen --input <thu_muc>."
        )
    fact_path = resolved_dir / "fact_sales.csv"
    if not fact_path.exists():
        raise FileNotFoundError(f"Khong thay {fact_path}.")

    df = pd.read_csv(fact_path, usecols=["invoice_no", "stock_code", "quantity"])
    print(f"[apriori_mba] Doc {len(df):,} dong tu {fact_path} (nguon: datalake/processed snapshot).")
    return df


def enrich_with_description(items: frozenset[str], description_map: dict[str, str]) -> str:
    return " | ".join(f"{code} ({description_map.get(code, 'khong co mo ta')})" for code in sorted(items))


def load_product_descriptions(source: str, input_dir: Path | None) -> dict[str, str]:
    """Lay StockCode -> Description de dien giai luat de doc, khong anh huong tinh toan."""
    try:
        if source == "dw":
            import psycopg2

            conn = psycopg2.connect(**_connection_params())
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT stock_code, description FROM dim_product;")
                    rows = cur.fetchall()
                return {code: (desc or "") for code, desc in rows}
            finally:
                conn.close()
        else:
            resolved_dir = input_dir or _latest_datalake_processed_dir()
            dim_path = resolved_dir / "dim_product.csv" if resolved_dir else None
            if dim_path and dim_path.exists():
                dim_df = pd.read_csv(dim_path)
                return dict(zip(dim_df["stock_code"], dim_df["description"].fillna("")))
    except Exception as exc:  # noqa: BLE001
        print(f"[apriori_mba] [canh bao] Khong lay duoc mo ta san pham: {exc}")
    return {}


# =============================================================================
# 2. Xay dung "gio hang" (basket) tu giao dich tho
# =============================================================================

def build_baskets(df: pd.DataFrame) -> list[frozenset[str]]:
    """1 hoa don = 1 basket = frozenset cac StockCode DUY NHAT trong hoa don do.

    Hoa don co nhieu dong cung 1 StockCode (vi du tach lo/so luong) duoc GOP
    thanh 1 item duy nhat - Apriori chuan quan tam su CO MAT, khong quan tam
    so luong/so dong lap lai.
    """
    grouped = df.groupby("invoice_no")["stock_code"].apply(lambda s: frozenset(s.astype(str)))
    baskets = [fs for fs in grouped.tolist() if fs]
    return baskets


# =============================================================================
# 3. Thuat toan Apriori (frequent itemsets) - khong one-hot encoding
# =============================================================================

def _generate_candidates(frequent_prev: list[frozenset[str]], k: int) -> set[frozenset[str]]:
    """Buoc "join + prune" chuan cua Apriori: ghep 2 itemset (k-1) phan tu co
    chung (k-2) phan tu dau (sau khi sap xep) thanh ung vien k phan tu, roi
    loai ung vien co bat ky tap con (k-1) phan tu nao KHONG pho bien (downward
    closure: moi tap con cua 1 itemset pho bien deu phai pho bien).
    """
    frequent_set = set(frequent_prev)
    sorted_tuples = sorted(tuple(sorted(fs)) for fs in frequent_prev)
    candidates: set[frozenset[str]] = set()
    n = len(sorted_tuples)
    prefix_len = k - 2  # voi k=2, prefix_len=0 -> moi cap deu "noi" duoc (dung = tat ca cap)

    for i in range(n):
        for j in range(i + 1, n):
            a, b = sorted_tuples[i], sorted_tuples[j]
            if a[:prefix_len] != b[:prefix_len]:
                break  # da sap xep -> cac j con lai cung khong khop prefix
            candidate = frozenset(a) | frozenset(b)
            if len(candidate) != k:
                continue
            if all(frozenset(sub) in frequent_set for sub in itertools.combinations(candidate, k - 1)):
                candidates.add(candidate)
    return candidates


def find_frequent_itemsets(
    baskets: list[frozenset[str]],
    min_support: float,
    max_len: int | None = DEFAULT_MAX_LEN,
) -> dict[frozenset[str], float]:
    """Tra ve dict {itemset (frozenset StockCode): support} cho MOI muc do dai
    tu 1 den max_len (hoac den khi khong con itemset pho bien nao). Khong
    dung one-hot: chi dem truc tiep tren danh sach basket (frozenset).
    """
    n = len(baskets)
    if n == 0 or not (0 < min_support <= 1):
        return {}

    counts: Counter[frozenset[str]] = Counter()
    for basket in baskets:
        for item in basket:
            counts[frozenset((item,))] += 1
    current = {fs: c / n for fs, c in counts.items() if c / n >= min_support}
    all_frequent: dict[frozenset[str], float] = dict(current)

    k = 2
    while current and (max_len is None or k <= max_len):
        candidates = _generate_candidates(list(current.keys()), k)
        if not candidates:
            break

        # Chi can xet item co mat trong it nhat 1 ung vien - thu nho basket
        # truoc khi sinh to hop, tranh quet toan bo basket lon khong lien quan.
        relevant_items = {item for cand in candidates for item in cand}

        counts = Counter()
        for basket in baskets:
            relevant = basket & relevant_items
            if len(relevant) < k:
                continue
            for combo in itertools.combinations(sorted(relevant), k):
                fs = frozenset(combo)
                if fs in candidates:
                    counts[fs] += 1

        current = {fs: c / n for fs, c in counts.items() if c / n >= min_support}
        all_frequent.update(current)
        k += 1

    return all_frequent


# =============================================================================
# 4. Sinh luat ket hop (association rules) tu frequent itemsets
# =============================================================================

def generate_rules(frequent: dict[frozenset[str], float], min_confidence: float) -> list[dict]:
    """Voi moi itemset pho bien kich thuoc >= 2, sinh moi luat A -> B (A, B la
    2 phan hoach khong rong cua itemset), giu lai luat co confidence >=
    min_confidence. support/confidence/lift tinh dung dinh nghia chuan:
      support(A->B)    = support(A u B)
      confidence(A->B) = support(A u B) / support(A)
      lift(A->B)       = confidence(A->B) / support(B)
    """
    rules: list[dict] = []
    if not (0 < min_confidence <= 1):
        return rules

    for itemset, support in frequent.items():
        if len(itemset) < 2:
            continue
        items = list(itemset)
        for r in range(1, len(items)):
            for antecedent_items in itertools.combinations(items, r):
                antecedent = frozenset(antecedent_items)
                consequent = itemset - antecedent
                if not consequent:
                    continue

                antecedent_support = frequent.get(antecedent)
                consequent_support = frequent.get(consequent)
                if antecedent_support is None or consequent_support is None:
                    # Khong the xay ra voi output cua find_frequent_itemsets()
                    # (downward closure dam bao moi tap con deu co mat), nhung
                    # giu guard de generate_rules() an toan khi goi doc lap
                    # (vd trong unit test) voi dict frequent khong day du.
                    continue

                confidence = support / antecedent_support
                if confidence < min_confidence:
                    continue
                lift = confidence / consequent_support if consequent_support > 0 else float("inf")

                rules.append(
                    {
                        "antecedent": antecedent,
                        "consequent": consequent,
                        "support": support,
                        "confidence": confidence,
                        "lift": lift,
                        "antecedent_support": antecedent_support,
                        "consequent_support": consequent_support,
                    }
                )
    rules.sort(key=lambda r: (r["lift"], r["confidence"]), reverse=True)
    return rules


# =============================================================================
# 5. CLI runner
# =============================================================================

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", choices=["dw", "csv"], default="dw",
                         help="Nguon du lieu: 'dw' (PostgreSQL DW qua SQL, mac dinh) hoac "
                              "'csv' (datalake/processed/<ngay>/fact_sales.csv, khong can Docker).")
    parser.add_argument("--input", type=Path, default=None,
                         help="Chi dung voi --source csv: thu muc datalake/processed/<ngay>/ cu the "
                              "(mac dinh: thu muc moi nhat tim thay).")
    parser.add_argument("--min-support", type=float, default=DEFAULT_MIN_SUPPORT,
                         help=f"Nguong support toi thieu (mac dinh {DEFAULT_MIN_SUPPORT}).")
    parser.add_argument("--min-confidence", type=float, default=DEFAULT_MIN_CONFIDENCE,
                         help=f"Nguong confidence toi thieu (mac dinh {DEFAULT_MIN_CONFIDENCE}).")
    parser.add_argument("--max-len", type=int, default=DEFAULT_MAX_LEN,
                         help=f"Do dai itemset toi da can xet (mac dinh {DEFAULT_MAX_LEN} = chi xet cap san pham; "
                              "tang len 3+ se cham hon dang ke do vai hoa don si/ban buon co rat nhieu dong).")
    parser.add_argument("--top-n", type=int, default=15, help="So luat in ra console (mac dinh 15).")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    load_env_file()
    ensure_dirs()
    LOCAL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    try:
        if args.source == "dw":
            df = load_transactions_from_dw()
            print(f"[apriori_mba] Doc {len(df):,} dong (invoice_no, stock_code) tu postgres-dw "
                  "(fact_sales JOIN dim_product).")
        else:
            df = load_transactions_from_csv(args.input)

        validate_transactions(df)

        baskets = build_baskets(df)
        n_baskets = len(baskets)
        n_items = len({item for b in baskets for item in b})
        print(f"[apriori_mba] So hoa don (basket): {n_baskets:,} | So san pham duy nhat xuat hien: {n_items:,}")
        print(f"[apriori_mba] Tham so: min_support={args.min_support}, min_confidence={args.min_confidence}, "
              f"max_len={args.max_len}")
        print(f"[apriori_mba]   -> min_support={args.min_support} tuong duong itemset phai xuat hien trong "
              f">= {int(args.min_support * n_baskets):,} / {n_baskets:,} hoa don.")

        if n_baskets == 0:
            raise RuntimeError("Khong co basket nao sau khi nhom theo invoice_no.")

        frequent = find_frequent_itemsets(baskets, args.min_support, args.max_len)
        n_by_size: dict[int, int] = {}
        for fs in frequent:
            n_by_size[len(fs)] = n_by_size.get(len(fs), 0) + 1
        print(f"[apriori_mba] Frequent itemsets theo kich thuoc: "
              f"{dict(sorted(n_by_size.items()))} (tong {len(frequent):,}).")

        if not frequent:
            print("[apriori_mba] [KET QUA] Khong tim thay frequent itemset nao voi nguong da chon - "
                  "thu giam --min-support. Khong co luat nao duoc sinh ra.")
            rules = []
        else:
            rules = generate_rules(frequent, args.min_confidence)
            print(f"[apriori_mba] So luat ket hop thoa min_confidence: {len(rules):,}")
            if rules:
                n_positive = sum(1 for r in rules if r["lift"] > 1)
                print(f"[apriori_mba]   -> {n_positive:,}/{len(rules):,} luat co lift > 1 "
                      "(lien he duong: mua A tang kha nang mua B so voi ngau nhien).")
            else:
                print("[apriori_mba] [KET QUA] Co frequent itemset nhung khong luat nao dat min_confidence - "
                      "thu giam --min-confidence.")

        description_map = load_product_descriptions(args.source, args.input)

        # --- Luu frequent itemsets ---
        itemsets_rows = [
            {
                "itemset_size": len(fs),
                "items": "|".join(sorted(fs)),
                "items_description": enrich_with_description(fs, description_map),
                "support": round(support, 6),
                "support_count": round(support * n_baskets),
            }
            for fs, support in frequent.items()
        ]
        itemsets_df = pd.DataFrame(itemsets_rows).sort_values(
            ["itemset_size", "support"], ascending=[True, False]
        ) if itemsets_rows else pd.DataFrame(
            columns=["itemset_size", "items", "items_description", "support", "support_count"]
        )
        itemsets_df.to_csv(ITEMSETS_OUTPUT_PATH, index=False)
        print(f"[apriori_mba] Da luu {len(itemsets_df):,} frequent itemset vao: {ITEMSETS_OUTPUT_PATH}")

        # --- Luu association rules ---
        rules_rows = [
            {
                "antecedent": "|".join(sorted(r["antecedent"])),
                "consequent": "|".join(sorted(r["consequent"])),
                "antecedent_description": enrich_with_description(r["antecedent"], description_map),
                "consequent_description": enrich_with_description(r["consequent"], description_map),
                "support": round(r["support"], 6),
                "confidence": round(r["confidence"], 6),
                "lift": round(r["lift"], 6),
                "antecedent_support": round(r["antecedent_support"], 6),
                "consequent_support": round(r["consequent_support"], 6),
                "is_positive_association": bool(r["lift"] > 1),
            }
            for r in rules
        ]
        rules_df = pd.DataFrame(rules_rows) if rules_rows else pd.DataFrame(
            columns=["antecedent", "consequent", "antecedent_description", "consequent_description",
                     "support", "confidence", "lift", "antecedent_support", "consequent_support",
                     "is_positive_association"]
        )
        rules_df.to_csv(RULES_OUTPUT_PATH, index=False)
        print(f"[apriori_mba] Da luu {len(rules_df):,} luat ket hop vao: {RULES_OUTPUT_PATH}")

        if rules:
            print(f"\n=== Top {min(args.top_n, len(rules))} luat theo lift (minh hoa, KHONG suy dien nhan qua) ===")
            for r in rules[: args.top_n]:
                ant = ", ".join(sorted(r["antecedent"]))
                con = ", ".join(sorted(r["consequent"]))
                print(f"  [{ant}] -> [{con}]  support={r['support']:.4f}  "
                      f"confidence={r['confidence']:.4f}  lift={r['lift']:.3f}")

        elapsed = time.time() - t0
        print(f"\n[apriori_mba] Hoan tat trong {elapsed:.1f}s.")

        log_ingest("apriori_mba", "success", len(rules_rows), str(RULES_OUTPUT_PATH))
        return 0
    except Exception as exc:  # noqa: BLE001
        log_ingest("apriori_mba", "failed", None, str(RULES_OUTPUT_PATH), error_message=str(exc))
        print(f"[LOI] apriori_mba that bai: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
