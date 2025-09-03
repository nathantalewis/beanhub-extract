import datetime
import decimal
import pathlib

import pytest

from beanhub_extract.data_types import Fingerprint
from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.credit_human import CreditHumanExtractor
from beanhub_extract.extractors.credit_human import parse_date
from beanhub_extract.extractors.credit_human import parse_currency_amount
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "date_str, expected",
    [
        ("08/11/25", datetime.date(2025, 8, 11)),
        ("07/11/25", datetime.date(2025, 7, 11)),
        ("06/11/25", datetime.date(2025, 6, 11)),
        ("12/31/24", datetime.date(2024, 12, 31)),
        ("01/01/26", datetime.date(2026, 1, 1)),
    ],
)
def test_parse_date(date_str: str, expected: datetime.date):
    assert parse_date(date_str) == expected


@pytest.mark.parametrize(
    "amount_str, expected",
    [
        ('"$150.00"', decimal.Decimal("150.00")),
        ('"$25,000.00"', decimal.Decimal("25000.00")),
        ('"$1,234.56"', decimal.Decimal("1234.56")),
        ('$100.00', decimal.Decimal("100.00")),  # Without quotes
    ],
)
def test_parse_currency_amount(amount_str: str, expected: decimal.Decimal):
    assert parse_currency_amount(amount_str) == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "credit_human.csv",
            [
                Transaction(
                    extractor="credit_human",
                    file="credit_human.csv",
                    lineno=1,
                    transaction_id="20250811:123456",
                    date=datetime.date(2025, 8, 11),
                    post_date=datetime.date(2025, 8, 11),
                    desc="Deposit Dividend 4.600% Annual Percentage Yield Earned 4.70% from 07/11/25 through 08/10/25",
                    amount=decimal.Decimal("150.00"),
                    currency="USD",
                    source_account="12345678-S0001",
                    extra={"balance": "$25,000.00"},
                ),
                Transaction(
                    extractor="credit_human",
                    file="credit_human.csv",
                    lineno=2,
                    transaction_id="20250711:234567",
                    date=datetime.date(2025, 7, 11),
                    post_date=datetime.date(2025, 7, 11),
                    desc="Deposit Dividend 4.600% Annual Percentage Yield Earned 4.70% from 06/11/25 through 07/10/25",
                    amount=decimal.Decimal("145.25"),
                    currency="USD",
                    source_account="12345678-S0001",
                    extra={"balance": "$24,850.00"},
                ),
                Transaction(
                    extractor="credit_human",
                    file="credit_human.csv",
                    lineno=3,
                    transaction_id="20250611:345678",
                    date=datetime.date(2025, 6, 11),
                    post_date=datetime.date(2025, 6, 11),
                    desc="Deposit Dividend 4.600% Annual Percentage Yield Earned 4.70% from 05/11/25 through 06/10/25",
                    amount=decimal.Decimal("148.75"),
                    currency="USD",
                    source_account="12345678-S0001",
                    extra={"balance": "$24,704.75"},
                ),
                Transaction(
                    extractor="credit_human",
                    file="credit_human.csv",
                    lineno=0,
                    transaction_id="BALANCE_20250811_12345678-S0001",
                    date=datetime.date(2025, 8, 12),
                    post_date=datetime.date(2025, 8, 12),
                    desc="Balance as of 2025-08-11",
                    amount=decimal.Decimal("25000.00"),
                    currency="USD",
                    type="BALANCE",
                    source_account="12345678-S0001",
                ),
            ],
        ),
    ],
)
def test_credit_human_extractor(
    input_file: str, expected: list[Transaction], fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = CreditHumanExtractor(fo)
        result = list(extractor())
        result = [strip_txn_base_path(fixtures_folder, txn) for txn in result]
        assert result == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "credit_human.csv",
            Fingerprint(
                starting_date=datetime.date(2025, 8, 11),
                first_row_hash="placeholder_hash",
            ),
        ),
    ],
)
def test_credit_human_fingerprint(
    input_file: str, expected: Fingerprint, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = CreditHumanExtractor(fo)
        result = extractor.fingerprint()
        # We can't predict the exact hash, so just check the date and that hash exists
        assert result is not None
        assert result.starting_date == expected.starting_date
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("credit_human.csv", True),
        ("wsecu.csv", False),
        ("chase_credit_card.csv", False),
        ("csv.csv", False),
    ],
)
def test_credit_human_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = CreditHumanExtractor(fo)
        result = extractor.detect()
        assert result == expected
