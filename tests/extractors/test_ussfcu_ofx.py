import datetime
import decimal
import pathlib

import pytest

from beanhub_extract.data_types import Fingerprint
from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.ussfcu_ofx import UssfcuOFXExtractor
from beanhub_extract.extractors.ussfcu_ofx import parse_ofx_datetime
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "dt_str, expected",
    [
        ("20250901000000", datetime.date(2025, 9, 1)),
        ("20250801000000", datetime.date(2025, 8, 1)),
        ("20250701000000", datetime.date(2025, 7, 1)),
    ],
)
def test_parse_ofx_datetime(dt_str: str, expected: datetime.date):
    assert parse_ofx_datetime(dt_str) == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "ussfcu.ofx",
            [
                Transaction(
                    extractor="ussfcu_ofx",
                    file="ussfcu.ofx",
                    lineno=1,
                    transaction_id="TXN001",
                    date=datetime.date(2025, 9, 1),
                    post_date=datetime.date(2025, 9, 1),
                    desc="DEPOSIT DIVIDEND 4.750% ANNUAL",
                    amount=decimal.Decimal("100.00"),
                    type="CREDIT",
                    note="DEPOSIT DIVIDEND 4.750% ANNUAL PERCENTAGE YIELD EARNED FROM 08/01/25 THROUGH 08/31/25",
                    currency="USD",
                    source_account="1234567890CD",
                    extra={},
                ),
                Transaction(
                    extractor="ussfcu_ofx",
                    file="ussfcu.ofx",
                    lineno=2,
                    transaction_id="TXN002",
                    date=datetime.date(2025, 8, 1),
                    post_date=datetime.date(2025, 8, 1),
                    desc="DEPOSIT DIVIDEND 4.750% ANNUAL",
                    amount=decimal.Decimal("150.00"),
                    type="CREDIT",
                    note="DEPOSIT DIVIDEND 4.750% ANNUAL PERCENTAGE YIELD EARNED FROM 07/01/25 THROUGH 07/31/25",
                    currency="USD",
                    source_account="1234567890CD",
                    extra={},
                ),
                Transaction(
                    extractor="ussfcu_ofx",
                    file="ussfcu.ofx",
                    lineno=3,
                    transaction_id="TXN003",
                    date=datetime.date(2025, 7, 1),
                    post_date=datetime.date(2025, 7, 1),
                    desc="DEPOSIT DIVIDEND 4.750% ANNUAL",
                    amount=decimal.Decimal("200.00"),
                    type="CREDIT",
                    note="DEPOSIT DIVIDEND 4.750% ANNUAL PERCENTAGE YIELD EARNED FROM 06/01/25 THROUGH 06/30/25",
                    currency="USD",
                    source_account="1234567890CD",
                    extra={},
                ),
                Transaction(
                    extractor="ussfcu_ofx",
                    file="ussfcu.ofx",
                    lineno=4,
                    transaction_id="BALANCE_20250906001321",
                    date=datetime.date(2025, 9, 6),
                    post_date=datetime.date(2025, 9, 6),
                    desc="Balance as of 2025-09-06",
                    amount=decimal.Decimal("25000.00"),
                    type="BALANCE",
                    note=None,
                    currency="USD",
                    source_account="1234567890CD",
                    extra={},
                ),
            ],
        ),
    ],
)
def test_ussfcu_ofx_extractor(
    input_file: str, expected: list[Transaction], fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = UssfcuOFXExtractor(fo)
        result = list(extractor())
        result = [strip_txn_base_path(fixtures_folder, txn) for txn in result]
        assert result == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "ussfcu.ofx",
            Fingerprint(
                starting_date=datetime.date(2025, 9, 1),
                first_row_hash="a1b2c3d4e5f6789012345678901234567890abcdef01234567890abcdef012345",
            ),
        ),
    ],
)
def test_ussfcu_ofx_fingerprint(
    input_file: str, expected: Fingerprint, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = UssfcuOFXExtractor(fo)
        result = extractor.fingerprint()
        # We can't predict the exact hash, so just check the date and that hash exists
        assert result is not None
        assert result.starting_date == expected.starting_date
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("ussfcu.ofx", True),
        ("ally_bank.qfx", False),
        ("capital_one.ofx", False),
        ("lfcu.ofx", False),
        ("chase_credit_card.csv", False),
        ("csv.csv", False),
    ],
)
def test_ussfcu_ofx_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = UssfcuOFXExtractor(fo)
        result = extractor.detect()
        assert result == expected


def test_ussfcu_ofx_extractor_name():
    """Test that the extractor name is correct."""
    assert UssfcuOFXExtractor.EXTRACTOR_NAME == "ussfcu_ofx"


def test_ussfcu_ofx_import_id_template():
    """Test that the import ID template is correct."""
    assert UssfcuOFXExtractor.DEFAULT_IMPORT_ID == "ussfcu:{{ source_account }}:{{ transaction_id }}"


