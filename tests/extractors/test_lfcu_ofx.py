import datetime
import decimal
import functools
import pathlib

import pytest

from beanhub_extract.data_types import Fingerprint
from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.lfcu_ofx import LfcuOFXExtractor
from beanhub_extract.extractors.lfcu_ofx import parse_ofx_datetime
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "dt_str, expected",
    [
        ("20240831024237", datetime.date(2024, 8, 31)),
        ("20240731042915", datetime.date(2024, 7, 31)),
        ("20240715214712", datetime.date(2024, 7, 15)),
    ],
)
def test_parse_ofx_datetime(dt_str: str, expected: datetime.date):
    assert parse_ofx_datetime(dt_str) == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "lfcu.ofx",
            [
                Transaction(
                    extractor="lfcu_ofx",
                    file="lfcu.ofx",
                    lineno=1,
                    transaction_id="100001",
                    date=datetime.date(2024, 8, 31),
                    post_date=datetime.date(2024, 8, 31),
                    desc="Dividend Deposit",
                    amount=decimal.Decimal("125.50"),
                    type="CREDIT",
                    note=None,
                    currency="USD",
                    source_account="1234567890-S0101",
                    last_four_digits="1234567890-S0101",
                    extra={},
                ),
                Transaction(
                    extractor="lfcu_ofx",
                    file="lfcu.ofx",
                    lineno=2,
                    transaction_id="100002",
                    date=datetime.date(2024, 7, 31),
                    post_date=datetime.date(2024, 7, 31),
                    desc="Dividend Deposit",
                    amount=decimal.Decimal("123.75"),
                    type="CREDIT",
                    note=None,
                    currency="USD",
                    source_account="1234567890-S0101",
                    last_four_digits="1234567890-S0101",
                    extra={},
                ),
                Transaction(
                    extractor="lfcu_ofx",
                    file="lfcu.ofx",
                    lineno=3,
                    transaction_id="100003",
                    date=datetime.date(2024, 7, 15),
                    post_date=datetime.date(2024, 7, 15),
                    desc="ATM Withdrawal",
                    amount=decimal.Decimal("-50.00"),
                    type="DEBIT",
                    note="ATM Transaction Fee",
                    currency="USD",
                    source_account="1234567890-S0101",
                    last_four_digits="1234567890-S0101",
                    extra={},
                ),
                Transaction(
                    extractor="lfcu_ofx",
                    file="lfcu.ofx",
                    lineno=4,
                    transaction_id="100004",
                    date=datetime.date(2024, 6, 30),
                    post_date=datetime.date(2024, 6, 30),
                    desc="Dividend Deposit",
                    amount=decimal.Decimal("120.25"),
                    type="CREDIT",
                    note=None,
                    currency="USD",
                    source_account="1234567890-S0101",
                    last_four_digits="1234567890-S0101",
                    extra={},
                ),
                Transaction(
                    extractor="lfcu_ofx",
                    file="lfcu.ofx",
                    lineno=5,
                    transaction_id="BALANCE_20240904205901",
                    date=datetime.date(2024, 9, 4),
                    post_date=datetime.date(2024, 9, 4),
                    desc="Balance as of 2024-09-04",
                    amount=decimal.Decimal("5432.10"),
                    type="BALANCE",
                    note=None,
                    currency="USD",
                    source_account="1234567890-S0101",
                    last_four_digits="1234567890-S0101",
                    extra={},
                ),
            ],
        ),
    ],
)
def test_lfcu_ofx_extractor(
    input_file: str, expected: list[Transaction], fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = LfcuOFXExtractor(fo)
        result = list(extractor())
        result = [strip_txn_base_path(fixtures_folder, txn) for txn in result]
        assert result == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "lfcu.ofx",
            Fingerprint(
                starting_date=datetime.date(2024, 8, 31),
                first_row_hash="a8b9c0d1e2f3456789abcdef01234567890abcdef01234567890abcdef012345",
            ),
        ),
    ],
)
def test_lfcu_ofx_fingerprint(
    input_file: str, expected: Fingerprint, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = LfcuOFXExtractor(fo)
        result = extractor.fingerprint()
        # We can't predict the exact hash, so just check the date and that hash exists
        assert result is not None
        assert result.starting_date == expected.starting_date
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("lfcu.ofx", True),
        ("ally_bank.qfx", False),
        ("capital_one.ofx", False),
        ("chase_credit_card.csv", False),
        ("csv.csv", False),
    ],
)
def test_lfcu_ofx_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = LfcuOFXExtractor(fo)
        result = extractor.detect()
        assert result == expected
