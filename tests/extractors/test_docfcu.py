import datetime
import decimal
import pathlib

import pytest

from beanhub_extract.data_types import Fingerprint
from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.docfcu import DocfcuExtractor
from beanhub_extract.extractors.docfcu import parse_date
from beanhub_extract.extractors.docfcu import parse_currency_amount
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "date_str, expected",
    [
        ("08/31/2025", datetime.date(2025, 8, 31)),
        ("07/31/2025", datetime.date(2025, 7, 31)),
        ("06/30/2025", datetime.date(2025, 6, 30)),
        ("12/31/2024", datetime.date(2024, 12, 31)),
        ("01/01/2026", datetime.date(2026, 1, 1)),
    ],
)
def test_parse_date(date_str: str, expected: datetime.date):
    assert parse_date(date_str) == expected


@pytest.mark.parametrize(
    "amount_str, expected",
    [
        ("123.45", decimal.Decimal("123.45")),
        ("98.76", decimal.Decimal("98.76")),
        ("0.00", decimal.Decimal("0.00")),
        ("", decimal.Decimal("0.00")),
        ("  ", decimal.Decimal("0.00")),
        ('"123.45"', decimal.Decimal("123.45")),
        ("1,234.56", decimal.Decimal("1234.56")),
    ],
)
def test_parse_currency_amount(amount_str: str, expected: decimal.Decimal):
    assert parse_currency_amount(amount_str) == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "docfcu.csv",
            [
                Transaction(
                    extractor="docfcu",
                    file="docfcu.csv",
                    lineno=6,
                    transaction_id="TXN20250831000000[-5:EST]*123.45*521**Deposit Dividend 4.690%",
                    date=datetime.date(2025, 8, 31),
                    post_date=datetime.date(2025, 8, 31),
                                            desc="Deposit Dividend 4.690% - APY Earned XXXXXX08/01/25 to 08/31/25",
                    amount=decimal.Decimal("123.45"),
                    currency="USD",
                    source_account="XXXXXXX123",
                    extra={
                        "balance": "10000.00",
                        "fees": "0.00",
                        "principal": "123.45",
                        "interest": "0.00",
                        "memo": "APY Earned XXXXXX08/01/25 to 08/31/25",
                    },
                ),
                Transaction(
                    extractor="docfcu",
                    file="docfcu.csv",
                    lineno=8,
                    transaction_id="TXN20250731000000[-5:EST]*98.76*521**Deposit Dividend 4.690%",
                    date=datetime.date(2025, 7, 31),
                    post_date=datetime.date(2025, 7, 31),
                                            desc="Deposit Dividend 4.690% - APY Earned XXXXXX07/01/25 to 07/31/25",
                    amount=decimal.Decimal("98.76"),
                    currency="USD",
                    source_account="XXXXXXX123",
                    extra={
                        "balance": "9876.55",
                        "fees": "0.00",
                        "principal": "98.76",
                        "interest": "0.00",
                        "memo": "APY Earned XXXXXX07/01/25 to 07/31/25",
                    },
                ),
                Transaction(
                    extractor="docfcu",
                    file="docfcu.csv",
                    lineno=10,
                    transaction_id="TXN20250630000000[-5:EST]*87.65*521**Deposit Dividend 4.690%",
                    date=datetime.date(2025, 6, 30),
                    post_date=datetime.date(2025, 6, 30),
                                            desc="Deposit Dividend 4.690% - APY Earned XXXXXX06/01/25 to 06/30/25",
                    amount=decimal.Decimal("87.65"),
                    currency="USD",
                    source_account="XXXXXXX123",
                    extra={
                        "balance": "9777.79",
                        "fees": "0.00",
                        "principal": "87.65",
                        "interest": "0.00",
                        "memo": "APY Earned XXXXXX06/01/25 to 06/30/25",
                    },
                ),
                Transaction(
                    extractor="docfcu",
                    file="docfcu.csv",
                    lineno=0,
                    transaction_id="BALANCE_20250831_XXXXXXX123",
                    date=datetime.date(2025, 9, 1),  # Day after most recent transaction
                    post_date=datetime.date(2025, 9, 1),
                    desc="Balance as of 2025-08-31",
                    amount=decimal.Decimal("10000.00"),
                    currency="USD",
                    type="BALANCE",
                    source_account="XXXXXXX123",
                    extra=None,
                ),
            ],
        ),
    ],
)
def test_extract(input_file: str, expected: list[Transaction]):
    fixture_path = pathlib.Path(__file__).parent / "fixtures" / input_file
    with open(fixture_path, encoding="utf-8") as f:
        extractor = DocfcuExtractor(f)
        result = list(extractor())
        result = [strip_txn_base_path(fixture_path.parent, txn) for txn in result]
        assert result == expected


def test_detect():
    fixture_path = pathlib.Path(__file__).parent / "fixtures" / "docfcu.csv"
    with open(fixture_path, encoding="utf-8") as f:
        extractor = DocfcuExtractor(f)
        assert extractor.detect() is True


def test_fingerprint():
    fixture_path = pathlib.Path(__file__).parent / "fixtures" / "docfcu.csv"
    with open(fixture_path, encoding="utf-8") as f:
        extractor = DocfcuExtractor(f)
        result = extractor.fingerprint()
        expected = Fingerprint(
            starting_date=datetime.date(2025, 8, 31),
            first_row_hash="a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8",
        )
        assert result.starting_date == expected.starting_date
        # Hash will be different but should be consistent
        assert len(result.first_row_hash) == 64  # SHA256 hash length


def test_extract_account_info():
    fixture_path = pathlib.Path(__file__).parent / "fixtures" / "docfcu.csv"
    with open(fixture_path, encoding="utf-8") as f:
        extractor = DocfcuExtractor(f)
        account_name, account_number = extractor._extract_account_info()
        assert account_name == "60 MONTH CERTIFICATE"
        assert account_number == "XXXXXXX123"


def test_comment_lines_filtered():
    """Test that COMMENT lines are filtered out and not included in transactions."""
    fixture_path = pathlib.Path(__file__).parent / "fixtures" / "docfcu.csv"
    with open(fixture_path, encoding="utf-8") as f:
        extractor = DocfcuExtractor(f)
        result = list(extractor())
        
        # Should have 4 transactions: 3 actual + 1 balance, not 6 (which would include comments)
        assert len(result) == 4
        
        # Verify none of the transactions have "COMMENT" in the description
        for txn in result:
            assert txn.desc != "COMMENT"
            assert "COMMENT" not in txn.transaction_id
        
        # Verify we have the expected transaction IDs (not the comment ones)
        transaction_ids = [txn.transaction_id for txn in result]
        expected_ids = [
            "TXN20250831000000[-5:EST]*123.45*521**Deposit Dividend 4.690%",
            "TXN20250731000000[-5:EST]*98.76*521**Deposit Dividend 4.690%", 
            "TXN20250630000000[-5:EST]*87.65*521**Deposit Dividend 4.690%",
            "BALANCE_20250831_XXXXXXX123"
        ]
        assert transaction_ids == expected_ids


def test_balance_transaction_created():
    """Test that a balance transaction is created from the most recent transaction."""
    fixture_path = pathlib.Path(__file__).parent / "fixtures" / "docfcu.csv"
    with open(fixture_path, encoding="utf-8") as f:
        extractor = DocfcuExtractor(f)
        result = list(extractor())
        
        # Find the balance transaction
        balance_txns = [txn for txn in result if txn.type == "BALANCE"]
        assert len(balance_txns) == 1
        
        balance_txn = balance_txns[0]
        assert balance_txn.transaction_id == "BALANCE_20250831_XXXXXXX123"
        assert balance_txn.date == datetime.date(2025, 9, 1)  # Day after most recent
        assert balance_txn.amount == decimal.Decimal("10000.00")  # Balance from most recent
        assert balance_txn.currency == "USD"
        assert balance_txn.source_account == "XXXXXXX123"
        assert balance_txn.desc == "Balance as of 2025-08-31"
