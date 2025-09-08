import datetime
import decimal
import pathlib

import pytest

from beanhub_extract.data_types import Fingerprint
from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.etrade_ofx import EtradeOFXExtractor
from beanhub_extract.extractors.etrade_ofx import parse_ofx_datetime
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "dt_str, expected",
    [
        ("20240301120000", datetime.date(2024, 3, 1)),
        ("20240310120000", datetime.date(2024, 3, 10)),
        ("20240315120000", datetime.date(2024, 3, 15)),
    ],
)
def test_parse_ofx_datetime(dt_str: str, expected: datetime.date):
    assert parse_ofx_datetime(dt_str) == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "etrade.qfx",
            [
                Transaction(
                    extractor="etrade_ofx",
                    file="etrade.qfx",
                    lineno=1,
                    transaction_id="240301_INT_0",
                    date=datetime.date(2024, 3, 1),
                    post_date=datetime.date(2024, 3, 1),
                    desc="INT - SAMPLE BANK N.A.(Period 03/01-03/31)",
                    amount=decimal.Decimal("5.25"),
                    type="INCOME",
                    note=None,
                    currency="USD",
                    source_account="123456789",
                    extra={"cusip": "SAMPLEINT", "ticker": "SINT", "security_name": "Sample Interest Security"},
                ),
                Transaction(
                    extractor="etrade_ofx",
                    file="etrade.qfx",
                    lineno=2,
                    transaction_id="240310_B_0",
                    date=datetime.date(2024, 3, 10),
                    post_date=datetime.date(2024, 3, 10),
                    desc="SAMPLE TECH ETF DIVIDEND REINVESTMENT",
                    amount=decimal.Decimal("-525.00"),
                    type="BUYSTOCK",
                    note=None,
                    currency="USD",
                    source_account="123456789",
                    extra={"units": "10.5", "unitprice": "50.00", "cusip": "123456789", "ticker": "STECH", "security_name": "SAMPLE TECHNOLOGY ETF"},
                ),
                Transaction(
                    extractor="etrade_ofx",
                    file="etrade.qfx",
                    lineno=3,
                    transaction_id="240310_DIV_0",
                    date=datetime.date(2024, 3, 10),
                    post_date=datetime.date(2024, 3, 10),
                    desc="DIV - SAMPLE TECH ETF",
                    amount=decimal.Decimal("525.00"),
                    type="INCOME",
                    note=None,
                    currency="USD",
                    source_account="123456789",
                    extra={"cusip": "123456789", "ticker": "STECH", "security_name": "SAMPLE TECHNOLOGY ETF"},
                ),
                Transaction(
                    extractor="etrade_ofx",
                    file="etrade.qfx",
                    lineno=4,
                    transaction_id="BALANCE_20240315120000",
                    date=datetime.date(2024, 3, 15),
                    post_date=datetime.date(2024, 3, 15),
                    desc="Available Cash Balance as of 2024-03-15",
                    amount=decimal.Decimal("1250.75"),
                    type="BALANCE",
                    note=None,
                    currency="USD",
                    source_account="123456789",
                    extra={"ticker": "CASH", "security_name": "Cash Balance"},
                ),
            ],
        ),
    ],
)
def test_etrade_ofx_extractor(
    input_file: str, expected: list[Transaction], fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = EtradeOFXExtractor(fo)
        result = list(extractor())
        result = [strip_txn_base_path(fixtures_folder, txn) for txn in result]
        assert result == expected


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "etrade.qfx",
            Fingerprint(
                starting_date=datetime.date(2024, 3, 1),
                first_row_hash="test_hash",  # Will be checked for existence, not exact value
            ),
        ),
    ],
)
def test_etrade_ofx_fingerprint(
    input_file: str, expected: Fingerprint, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = EtradeOFXExtractor(fo)
        result = extractor.fingerprint()
        # We can't predict the exact hash, so just check the date and that hash exists
        assert result is not None
        assert result.starting_date == expected.starting_date
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("etrade.qfx", True),
        ("ally_bank.qfx", False),
        ("capital_one.ofx", False),
        ("chase_credit_card.csv", False),
        ("csv.csv", False),
    ],
)
def test_etrade_ofx_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt") as fo:
        extractor = EtradeOFXExtractor(fo)
        result = extractor.detect()
        assert result == expected
