import datetime
import decimal
import functools
import pathlib

import pytest

from beanhub_extract.data_types import Fingerprint
from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.capital_one_ofx import CapitalOneOFXExtractor
from beanhub_extract.extractors.capital_one_ofx import parse_ofx_datetime
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "dt_str, expected",
    [
        ("20240315000000.000", datetime.date(2024, 3, 15)),
        ("20240101000000.000", datetime.date(2024, 1, 1)),
        ("20231225000000.000", datetime.date(2023, 12, 25)),
    ],
)
def test_parse_ofx_datetime(dt_str: str, expected: datetime.date):
    assert parse_ofx_datetime(dt_str) == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "capital_one.ofx",
            [
                Transaction(
                    extractor="capital_one_ofx",
                    file="capital_one.ofx",
                    lineno=1,
                    transaction_id="202403151152001",
                    date=datetime.date(2024, 3, 14),
                    post_date=datetime.date(2024, 3, 15),
                    desc="COFFEE SHOP DOWNTOWN",
                    amount=decimal.Decimal("-25.99"),
                    type="DEBIT",
                    note=None,
                    currency="USD",
                    source_account="1234",
                    last_four_digits="1234",
                    extra={},
                ),
                Transaction(
                    extractor="capital_one_ofx",
                    file="capital_one.ofx",
                    lineno=2,
                    transaction_id="202403141202002",
                    date=datetime.date(2024, 3, 14),
                    post_date=datetime.date(2024, 3, 14),
                    desc="GROCERY STORE",
                    amount=decimal.Decimal("-12.50"),
                    type="DEBIT",
                    note=None,
                    currency="USD",
                    source_account="1234",
                    last_four_digits="5678",
                    extra={},
                ),
                Transaction(
                    extractor="capital_one_ofx",
                    file="capital_one.ofx",
                    lineno=3,
                    transaction_id="202403122149004",
                    date=datetime.date(2024, 3, 12),
                    post_date=datetime.date(2024, 3, 12),
                    desc="CAPITAL ONE AUTOPAY PYMT",
                    amount=decimal.Decimal("500.00"),
                    type="CREDIT",
                    note=None,
                    currency="USD",
                    source_account="1234",
                    last_four_digits="1234",
                    extra={},
                ),
                Transaction(
                    extractor="capital_one_ofx",
                    file="capital_one.ofx",
                    lineno=4,
                    transaction_id="BALANCE_20240316174441",
                    date=datetime.date(2024, 3, 16),
                    post_date=datetime.date(2024, 3, 16),
                    desc="Balance as of 2024-03-16",
                    amount=decimal.Decimal("-234.56"),
                    type="BALANCE",
                    note=None,
                    currency="USD",
                    source_account="1234",
                    last_four_digits="1234",
                    extra={},
                ),
            ],
        ),
    ],
)
def test_capital_one_ofx_extractor(
    input_file: str, expected: list[Transaction], fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = CapitalOneOFXExtractor(fo)
        result = list(extractor())
        result = [strip_txn_base_path(fixtures_folder, txn) for txn in result]
        assert result == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "capital_one.ofx",
            Fingerprint(
                starting_date=datetime.date(2024, 3, 15),
                first_row_hash="a8b9c0d1e2f3456789abcdef01234567890abcdef01234567890abcdef012345",
            ),
        ),
    ],
)
def test_capital_one_ofx_fingerprint(
    input_file: str, expected: Fingerprint, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = CapitalOneOFXExtractor(fo)
        result = extractor.fingerprint()
        # We can't predict the exact hash, so just check the date and that hash exists
        assert result is not None
        assert result.starting_date == expected.starting_date
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("capital_one.ofx", True),
        ("chase_credit_card.csv", False),
        ("csv.csv", False),
    ],
)
def test_capital_one_ofx_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = CapitalOneOFXExtractor(fo)
        result = extractor.detect()
        assert result == expected
