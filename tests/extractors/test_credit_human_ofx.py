import datetime
import decimal
import pathlib

import pytest

from beanhub_extract.data_types import Fingerprint
from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.credit_human_ofx import CreditHumanOFXExtractor
from beanhub_extract.extractors.credit_human_ofx import parse_ofx_datetime
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "dt_str, expected",
    [
        ("20251011", datetime.date(2025, 10, 11)),
        ("20250911", datetime.date(2025, 9, 11)),
        ("20250811", datetime.date(2025, 8, 11)),
    ],
)
def test_parse_ofx_datetime(dt_str: str, expected: datetime.date):
    assert parse_ofx_datetime(dt_str) == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "credit_human.ofx",
            [
                Transaction(
                    extractor="credit_human_ofx",
                    file="credit_human.ofx",
                    lineno=1,
                    transaction_id="TEST20251011:001",
                    date=datetime.date(2025, 10, 11),
                    post_date=datetime.date(2025, 10, 11),
                    desc="2.500% Annual Percentage Yield Earned",
                    amount=decimal.Decimal("100.00"),
                    type="CREDIT",
                    note="Deposit Dividend 2.500% Annual Percentage Yield Earned 2.55% from 09/11/25 through 10/10/25",
                    currency="USD",
                    source_account="TEST123456-S0001",
                    extra={},
                ),
                Transaction(
                    extractor="credit_human_ofx",
                    file="credit_human.ofx",
                    lineno=2,
                    transaction_id="TEST20250911:002",
                    date=datetime.date(2025, 9, 11),
                    post_date=datetime.date(2025, 9, 11),
                    desc="2.500% Annual Percentage Yield Earned",
                    amount=decimal.Decimal("150.00"),
                    type="CREDIT",
                    note="Deposit Dividend 2.500% Annual Percentage Yield Earned 2.55% from 08/11/25 through 09/10/25",
                    currency="USD",
                    source_account="TEST123456-S0001",
                    extra={},
                ),
                Transaction(
                    extractor="credit_human_ofx",
                    file="credit_human.ofx",
                    lineno=3,
                    transaction_id="TEST20250811:003",
                    date=datetime.date(2025, 8, 11),
                    post_date=datetime.date(2025, 8, 11),
                    desc="2.500% Annual Percentage Yield Earned",
                    amount=decimal.Decimal("200.00"),
                    type="CREDIT",
                    note="Deposit Dividend 2.500% Annual Percentage Yield Earned 2.55% from 07/11/25 through 08/10/25",
                    currency="USD",
                    source_account="TEST123456-S0001",
                    extra={},
                ),
                Transaction(
                    extractor="credit_human_ofx",
                    file="credit_human.ofx",
                    lineno=4,
                    transaction_id="BALANCE_20251102001433",
                    date=datetime.date(2025, 11, 2),
                    post_date=datetime.date(2025, 11, 2),
                    desc="Balance as of 2025-11-02",
                    amount=decimal.Decimal("10000.00"),
                    type="BALANCE",
                    note=None,
                    currency="USD",
                    source_account="TEST123456-S0001",
                    extra={},
                ),
            ],
        ),
    ],
)
def test_credit_human_ofx_extractor(
    input_file: str, expected: list[Transaction], fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = CreditHumanOFXExtractor(fo)
        result = list(extractor())
        result = [strip_txn_base_path(fixtures_folder, txn) for txn in result]
        assert result == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "credit_human.ofx",
            Fingerprint(
                starting_date=datetime.date(2025, 10, 11),
                first_row_hash="a1b2c3d4e5f6789012345678901234567890abcdef01234567890abcdef012345",
            ),
        ),
    ],
)
def test_credit_human_ofx_fingerprint(
    input_file: str, expected: Fingerprint, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = CreditHumanOFXExtractor(fo)
        result = extractor.fingerprint()
        assert result is not None
        assert result.starting_date == expected.starting_date
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("credit_human.ofx", True),
        ("ussfcu.ofx", False),
        ("ally_bank.qfx", False),
        ("capital_one.ofx", False),
        ("lfcu.ofx", False),
        ("chase_credit_card.csv", False),
        ("csv.csv", False),
    ],
)
def test_credit_human_ofx_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = CreditHumanOFXExtractor(fo)
        result = extractor.detect()
        assert result == expected


def test_credit_human_ofx_extractor_name():
    """Test that the extractor name is correct."""
    assert CreditHumanOFXExtractor.EXTRACTOR_NAME == "credit_human_ofx"


def test_credit_human_ofx_import_id_template():
    """Test that the import ID template is correct."""
    assert CreditHumanOFXExtractor.DEFAULT_IMPORT_ID == "credit_human:{{ source_account }}:{{ transaction_id }}"

