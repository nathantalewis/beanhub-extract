import datetime
import decimal
import pathlib

import pytest

from beanhub_extract.data_types import Fingerprint
from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.docfcu_ofx import DocfcuOFXExtractor
from beanhub_extract.extractors.docfcu_ofx import parse_ofx_datetime
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "dt_str, expected",
    [
        ("20251031", datetime.date(2025, 10, 31)),
        ("20250930", datetime.date(2025, 9, 30)),
        ("20250831", datetime.date(2025, 8, 31)),
    ],
)
def test_parse_ofx_datetime(dt_str: str, expected: datetime.date):
    assert parse_ofx_datetime(dt_str) == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "docfcu.ofx",
            [
                Transaction(
                    extractor="docfcu_ofx",
                    file="docfcu.ofx",
                    lineno=1,
                    transaction_id="TEST2025-10-31Some(001)",
                    date=datetime.date(2025, 10, 31),
                    post_date=datetime.date(2025, 10, 31),
                    desc="DEPOSIT DIVIDEND 2.500% ANNUAL P",
                    amount=decimal.Decimal("100.00"),
                    type="CREDIT",
                    note="DEPOSIT DIVIDEND 2.500% ANNUAL PERCENTAGE YIELD EARNED 2.55% FROM 10/01/25 THROUGH 10/31/25",
                    currency="USD",
                    source_account="TEST123456S40",
                    extra={},
                ),
                Transaction(
                    extractor="docfcu_ofx",
                    file="docfcu.ofx",
                    lineno=2,
                    transaction_id="BALANCE_20251107164449",
                    date=datetime.date(2025, 11, 7),
                    post_date=datetime.date(2025, 11, 7),
                    desc="Balance as of 2025-11-07",
                    amount=decimal.Decimal("10000.00"),
                    type="BALANCE",
                    note=None,
                    currency="USD",
                    source_account="TEST123456S40",
                    extra={},
                ),
            ],
        ),
    ],
)
def test_docfcu_ofx_extractor(
    input_file: str, expected: list[Transaction], fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = DocfcuOFXExtractor(fo)
        result = list(extractor())
        result = [strip_txn_base_path(fixtures_folder, txn) for txn in result]
        assert result == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "docfcu.ofx",
            Fingerprint(
                starting_date=datetime.date(2025, 10, 31),
                first_row_hash="a1b2c3d4e5f6789012345678901234567890abcdef01234567890abcdef012345",
            ),
        ),
    ],
)
def test_docfcu_ofx_fingerprint(
    input_file: str, expected: Fingerprint, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = DocfcuOFXExtractor(fo)
        result = extractor.fingerprint()
        assert result is not None
        assert result.starting_date == expected.starting_date
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("docfcu.ofx", True),
        ("ussfcu.ofx", False),
        ("ally_bank.qfx", False),
        ("capital_one.ofx", False),
        ("lfcu.ofx", False),
        ("credit_human.ofx", False),
        ("chase_credit_card.csv", False),
        ("csv.csv", False),
    ],
)
def test_docfcu_ofx_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = DocfcuOFXExtractor(fo)
        result = extractor.detect()
        assert result == expected


def test_docfcu_ofx_extractor_name():
    """Test that the extractor name is correct."""
    assert DocfcuOFXExtractor.EXTRACTOR_NAME == "docfcu_ofx"


def test_docfcu_ofx_import_id_template():
    """Test that the import ID template is correct."""
    assert DocfcuOFXExtractor.DEFAULT_IMPORT_ID == "docfcu:{{ source_account }}:{{ transaction_id }}"

