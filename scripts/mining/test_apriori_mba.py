"""Unit test cho scripts/mining/apriori_mba.py (Phase 8.1 - Apriori MBA).

Dung unittest (thu vien chuan Python) - KHONG them pytest/moi dependency nao,
du an hien chua co framework test nao san co.

Chay (tu thu muc goc repo - du an khong dung package __init__.py nen chay
truc tiep nhu script, KHONG dung "python -m unittest scripts.mining...."):
    uv run scripts/mining/test_apriori_mba.py -v
    # hoac:
    .venv/Scripts/python.exe scripts/mining/test_apriori_mba.py -v
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from apriori_mba import (  # noqa: E402
    build_baskets,
    find_frequent_itemsets,
    generate_rules,
    validate_transactions,
)


class TestBuildBaskets(unittest.TestCase):
    def test_groups_by_invoice_and_dedupes_items(self) -> None:
        # Hoa don INV1 co StockCode "A" lap lai 2 dong (vd tach lo) - phai gop
        # thanh 1 item duy nhat trong basket.
        df = pd.DataFrame(
            {
                "invoice_no": ["INV1", "INV1", "INV1", "INV2"],
                "stock_code": ["A", "B", "A", "C"],
            }
        )
        baskets = build_baskets(df)
        self.assertEqual(len(baskets), 2)
        self.assertIn(frozenset({"A", "B"}), baskets)
        self.assertIn(frozenset({"C"}), baskets)


class TestValidateTransactions(unittest.TestCase):
    def test_accepts_clean_data(self) -> None:
        df = pd.DataFrame({"invoice_no": ["489434", "489435"], "stock_code": ["A", "B"], "quantity": [1, 2]})
        validate_transactions(df)  # khong duoc raise

    def test_rejects_missing_columns(self) -> None:
        df = pd.DataFrame({"invoice_no": ["489434"]})
        with self.assertRaises(ValueError):
            validate_transactions(df)

    def test_rejects_empty_dataframe(self) -> None:
        df = pd.DataFrame({"invoice_no": [], "stock_code": []})
        with self.assertRaises(ValueError):
            validate_transactions(df)

    def test_rejects_cancelled_invoices(self) -> None:
        # Invoice bat dau bang "C" = don huy - phai da duoc loai truoc (quy tac 0.3)
        df = pd.DataFrame({"invoice_no": ["C489434", "489435"], "stock_code": ["A", "B"]})
        with self.assertRaises(ValueError) as ctx:
            validate_transactions(df)
        self.assertIn("huy", str(ctx.exception))

    def test_rejects_non_positive_quantity(self) -> None:
        df = pd.DataFrame({"invoice_no": ["489434", "489435"], "stock_code": ["A", "B"], "quantity": [1, 0]})
        with self.assertRaises(ValueError) as ctx:
            validate_transactions(df)
        self.assertIn("Quantity", str(ctx.exception))

    def test_rejects_null_item_or_invoice(self) -> None:
        df = pd.DataFrame({"invoice_no": ["489434", None], "stock_code": ["A", "B"]})
        with self.assertRaises(ValueError):
            validate_transactions(df)


class TestFindFrequentItemsets(unittest.TestCase):
    """5 basket co san - ket qua hand-computed, doi chieu dung tung gia tri."""

    BASKETS = [
        frozenset({"A", "B", "C"}),
        frozenset({"A", "B"}),
        frozenset({"A", "C"}),
        frozenset({"B", "C"}),
        frozenset({"A", "B", "D"}),
    ]
    # support(A)=4/5=0.8 support(B)=4/5=0.8 support(C)=3/5=0.6 support(D)=1/5=0.2
    # support(AB)=3/5=0.6 support(AC)=2/5=0.4 support(BC)=2/5=0.4 support(AD)=1/5=0.2 support(BD)=1/5=0.2
    # support(ABC)=1/5=0.2 support(ABD)=1/5=0.2

    def test_support_values_match_hand_calculation(self) -> None:
        freq = find_frequent_itemsets(self.BASKETS, min_support=0.4, max_len=3)
        expected = {
            frozenset({"A"}): 0.8,
            frozenset({"B"}): 0.8,
            frozenset({"C"}): 0.6,
            frozenset({"A", "B"}): 0.6,
            frozenset({"A", "C"}): 0.4,
            frozenset({"B", "C"}): 0.4,
        }
        self.assertEqual(freq, expected)
        # D co support 0.2 < 0.4 -> khong duoc liet ke, va theo downward
        # closure, khong co itemset nao chua D duoc xet tiep (vd AD/BD/ABD).
        self.assertNotIn(frozenset({"D"}), freq)
        self.assertFalse(any("D" in fs for fs in freq))

    def test_max_len_limits_itemset_size(self) -> None:
        freq = find_frequent_itemsets(self.BASKETS, min_support=0.1, max_len=1)
        self.assertTrue(all(len(fs) == 1 for fs in freq))

    def test_empty_baskets_returns_empty_dict(self) -> None:
        self.assertEqual(find_frequent_itemsets([], min_support=0.1), {})

    def test_min_support_too_high_returns_empty_dict(self) -> None:
        # Khong co itemset nao xuat hien trong 100% basket (A xuat hien 0.8) -> rong.
        freq = find_frequent_itemsets(self.BASKETS, min_support=1.0, max_len=3)
        self.assertEqual(freq, {})

    def test_invalid_min_support_returns_empty_dict(self) -> None:
        self.assertEqual(find_frequent_itemsets(self.BASKETS, min_support=0.0), {})
        self.assertEqual(find_frequent_itemsets(self.BASKETS, min_support=1.5), {})


class TestGenerateRules(unittest.TestCase):
    def test_confidence_and_lift_match_hand_calculation(self) -> None:
        # support(A)=0.8, support(B)=0.8, support(AB)=0.6
        # confidence(A->B) = 0.6/0.8 = 0.75 ; lift = 0.75/0.8 = 0.9375
        frequent = {
            frozenset({"A"}): 0.8,
            frozenset({"B"}): 0.8,
            frozenset({"A", "B"}): 0.6,
        }
        rules = generate_rules(frequent, min_confidence=0.5)
        self.assertEqual(len(rules), 2)  # A->B va B->A
        rule_ab = next(r for r in rules if r["antecedent"] == frozenset({"A"}))
        self.assertAlmostEqual(rule_ab["confidence"], 0.75, places=6)
        self.assertAlmostEqual(rule_ab["lift"], 0.9375, places=6)
        self.assertAlmostEqual(rule_ab["support"], 0.6, places=6)

    def test_lift_greater_than_one_means_positive_association(self) -> None:
        # support(A)=0.2, support(B)=0.2, support(AB)=0.2 (A va B LUON di cung nhau)
        # confidence(A->B)=1.0 ; lift = 1.0/0.2 = 5.0 > 1 -> lien he duong
        frequent = {
            frozenset({"A"}): 0.2,
            frozenset({"B"}): 0.2,
            frozenset({"A", "B"}): 0.2,
        }
        rules = generate_rules(frequent, min_confidence=0.5)
        rule_ab = next(r for r in rules if r["antecedent"] == frozenset({"A"}))
        self.assertGreater(rule_ab["lift"], 1.0)
        self.assertAlmostEqual(rule_ab["lift"], 5.0, places=6)

    def test_min_confidence_filters_weak_rules(self) -> None:
        frequent = {
            frozenset({"A"}): 0.8,
            frozenset({"B"}): 0.8,
            frozenset({"A", "B"}): 0.1,  # confidence(A->B) = 0.1/0.8 = 0.125
        }
        rules = generate_rules(frequent, min_confidence=0.9)
        self.assertEqual(rules, [])

    def test_no_frequent_itemsets_yields_no_rules(self) -> None:
        self.assertEqual(generate_rules({}, min_confidence=0.3), [])

    def test_itemsets_of_size_one_produce_no_rules(self) -> None:
        frequent = {frozenset({"A"}): 0.8, frozenset({"B"}): 0.5}
        self.assertEqual(generate_rules(frequent, min_confidence=0.1), [])

    def test_missing_subset_support_is_skipped_defensively(self) -> None:
        # Truong hop khong the xay ra tu find_frequent_itemsets() (downward
        # closure luon dam bao du subset), nhung generate_rules() phai an toan
        # khi duoc goi doc lap (vd trong test) voi dict thieu du lieu.
        frequent = {frozenset({"A", "B"}): 0.5}  # thieu support({"A"}) va support({"B"})
        rules = generate_rules(frequent, min_confidence=0.1)
        self.assertEqual(rules, [])

    def test_invalid_min_confidence_returns_empty(self) -> None:
        frequent = {frozenset({"A"}): 0.8, frozenset({"B"}): 0.8, frozenset({"A", "B"}): 0.6}
        self.assertEqual(generate_rules(frequent, min_confidence=0.0), [])
        self.assertEqual(generate_rules(frequent, min_confidence=1.5), [])


if __name__ == "__main__":
    unittest.main()
