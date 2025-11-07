import datetime
import decimal
import pathlib

import pytest

from beanhub_extract.data_types import Fingerprint
from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.wsecu_ofx import WsecuOFXExtractor
from beanhub_extract.extractors.wsecu_ofx import parse_ofx_datetime
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "dt_str, expected",
    [
        ("20251103", datetime.date(2025, 11, 3)),
        ("20251030", datetime.date(2025, 10, 30)),
        ("20251028", datetime.date(2025, 10, 28)),
    ],
)
def test_parse_ofx_datetime(dt_str: str, expected: datetime.date):
    assert parse_ofx_datetime(dt_str) == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "wsecu.ofx",
            [
                Transaction(
                    extractor="wsecu_ofx",
                    file="wsecu.ofx",
                    lineno=1,
                    transaction_id="TEST20251103001",
                    date=datetime.date(2025, 11, 3),
                    post_date=datetime.date(2025, 11, 3),
                    desc="Sample Payment A",
                    amount=decimal.Decimal("-100.00"),
                    type="DEBIT",
                    note="SAMPLE PAYMENT A",
                    currency="USD",
                    source_account="TEST123456-S09",
                    extra={},
                ),
                Transaction(
                    extractor="wsecu_ofx",
                    file="wsecu.ofx",
                    lineno=2,
                    transaction_id="TEST20251030001",
                    date=datetime.date(2025, 10, 30),
                    post_date=datetime.date(2025, 10, 30),
                    desc="Sample Deposit A",
                    amount=decimal.Decimal("200.00"),
                    type="CREDIT",
                    note="SAMPLE DEPOSIT A",
                    currency="USD",
                    source_account="TEST123456-S09",
                    extra={},
                ),
                Transaction(
                    extractor="wsecu_ofx",
                    file="wsecu.ofx",
                    lineno=3,
                    transaction_id="TEST20251028001",
                    date=datetime.date(2025, 10, 28),
                    post_date=datetime.date(2025, 10, 28),
                    desc="Sample Transfer A",
                    amount=decimal.Decimal("1000.00"),
                    type="CREDIT",
                    note="SAMPLE TRANSFER A FROM ACCOUNT XYZ123",
                    currency="USD",
                    source_account="TEST123456-S09",
                    extra={},
                ),
                Transaction(
                    extractor="wsecu_ofx",
                    file="wsecu.ofx",
                    lineno=4,
                    transaction_id="TEST20251027001",
                    date=datetime.date(2025, 10, 27),
                    post_date=datetime.date(2025, 10, 27),
                    desc="Sample Transfer B",
                    amount=decimal.Decimal("2500.00"),
                    type="CREDIT",
                    note="SAMPLE TRANSFER B FROM ACCOUNT ABC456",
                    currency="USD",
                    source_account="TEST123456-S09",
                    extra={},
                ),
                Transaction(
                    extractor="wsecu_ofx",
                    file="wsecu.ofx",
                    lineno=5,
                    transaction_id="TEST20250930001",
                    date=datetime.date(2025, 9, 30),
                    post_date=datetime.date(2025, 9, 30),
                    desc="Sample Payment B",
                    amount=decimal.Decimal("-400.00"),
                    type="DEBIT",
                    note="SAMPLE PAYMENT B",
                    currency="USD",
                    source_account="TEST123456-S09",
                    extra={},
                ),
                Transaction(
                    extractor="wsecu_ofx",
                    file="wsecu.ofx",
                    lineno=6,
                    transaction_id="TEST20250928001",
                    date=datetime.date(2025, 9, 28),
                    post_date=datetime.date(2025, 9, 28),
                    desc="Sample Deposit B",
                    amount=decimal.Decimal("400.00"),
                    type="CREDIT",
                    note="SAMPLE DEPOSIT B",
                    currency="USD",
                    source_account="TEST123456-S09",
                    extra={},
                ),
                Transaction(
                    extractor="wsecu_ofx",
                    file="wsecu.ofx",
                    lineno=7,
                    transaction_id="BALANCE_20251107064122",
                    date=datetime.date(2025, 11, 7),
                    post_date=datetime.date(2025, 11, 7),
                    desc="Balance as of 2025-11-07",
                    amount=decimal.Decimal("10000.00"),
                    type="BALANCE",
                    note=None,
                    currency="USD",
                    source_account="TEST123456-S09",
                    extra={},
                ),
            ],
        ),
    ],
)
def test_wsecu_ofx_extractor(
    input_file: str, expected: list[Transaction], fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = WsecuOFXExtractor(fo)
        result = list(extractor())
        result = [strip_txn_base_path(fixtures_folder, txn) for txn in result]
        assert result == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "wsecu.ofx",
            Fingerprint(
                starting_date=datetime.date(2025, 11, 3),
                first_row_hash="a1b2c3d4e5f6789012345678901234567890abcdef01234567890abcdef012345",
            ),
        ),
    ],
)
def test_wsecu_ofx_fingerprint(
    input_file: str, expected: Fingerprint, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = WsecuOFXExtractor(fo)
        result = extractor.fingerprint()
        assert result is not None
        assert result.starting_date == expected.starting_date
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("wsecu.ofx", True),
        ("ussfcu.ofx", False),
        ("ally_bank.qfx", False),
        ("capital_one.ofx", False),
        ("lfcu.ofx", False),
        ("credit_human.ofx", False),
        ("docfcu.ofx", False),
        ("chase_credit_card.csv", False),
        ("csv.csv", False),
    ],
)
def test_wsecu_ofx_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = WsecuOFXExtractor(fo)
        result = extractor.detect()
        assert result == expected


def test_wsecu_ofx_extractor_name():
    """Test that the extractor name is correct."""
    assert WsecuOFXExtractor.EXTRACTOR_NAME == "wsecu_ofx"


def test_wsecu_ofx_import_id_template():
    """Test that the import ID template is correct."""
    assert WsecuOFXExtractor.DEFAULT_IMPORT_ID == "wsecu:{{ source_account }}:{{ transaction_id }}"

