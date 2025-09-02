import datetime
import pathlib

import pytest

from beanhub_extract.extractors.bac_san_jose_credit import BacSanJoseCreditExtractor, parse_date


@pytest.mark.parametrize(
    "date_str, expected",
    [
        ("05/03/2025", datetime.date(2025, 3, 5)),
        ("27/12/2024", datetime.date(2024, 12, 27)),
        ("01/01/2025", datetime.date(2025, 1, 1)),
    ],
)
def test_parse_date(date_str: str, expected: datetime.date):
    assert parse_date(date_str) == expected


def test_bac_san_jose_credit_extractor(fixtures_folder: pathlib.Path):
    """Test that the BAC San José credit extractor can parse transactions correctly."""
    input_file_path = fixtures_folder / "bac_san_jose_credit.csv"
    with input_file_path.open("rt", encoding="utf-8") as fo:
        extractor = BacSanJoseCreditExtractor(fo)
        result = list(extractor())
        
        # Should have 6 transactions (5 regular + 1 balance)
        assert len(result) == 6
        
        # Check that we have the expected transaction types
        regular_transactions = [txn for txn in result if txn.type != "BALANCE"]
        balance_transactions = [txn for txn in result if txn.type == "BALANCE"]
        
        assert len(regular_transactions) == 5
        assert len(balance_transactions) == 1
        
        # Check some key properties
        assert all(txn.extractor == "bac_san_jose_credit" for txn in result)
        assert all(txn.currency == "CRC" for txn in result)
        assert all(txn.source_account == "9999" for txn in result)
        
        # Check that we have transactions for both cards (9999 and 8888)
        last_four_digits = {txn.last_four_digits for txn in result}
        assert "9999" in last_four_digits
        assert "8888" in last_four_digits
        
        # Check that amounts are negative (liability account convention)
        assert all(txn.amount < 0 for txn in result)
        
        # Check specific transaction descriptions
        descriptions = {txn.desc for txn in regular_transactions}
        assert "SUPER MARKET TEST" in descriptions
        assert "A Y A PAR 1234567" in descriptions
        assert "ICE PAR 12345678" in descriptions


def test_bac_san_jose_credit_fingerprint(fixtures_folder: pathlib.Path):
    """Test that the BAC San José credit extractor generates a valid fingerprint."""
    input_file_path = fixtures_folder / "bac_san_jose_credit.csv"
    with input_file_path.open("rt", encoding="utf-8") as fo:
        extractor = BacSanJoseCreditExtractor(fo)
        result = extractor.fingerprint()
        
        # We can't predict the exact hash, so just verify the structure
        assert result is not None
        assert result.starting_date == datetime.date(2025, 3, 27)
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("bac_san_jose_credit.csv", True),
        ("bac_san_jose_bank.csv", False),
        ("csv.csv", False),
        ("empty.csv", False),
    ],
)
def test_bac_san_jose_credit_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt", encoding="utf-8") as fo:
        extractor = BacSanJoseCreditExtractor(fo)
        assert extractor.detect() == expected


def test_bac_san_jose_credit_extractor_name():
    """Test that the extractor has the correct name."""
    assert BacSanJoseCreditExtractor.EXTRACTOR_NAME == "bac_san_jose_credit"


def test_bac_san_jose_credit_import_id_template():
    """Test that the import ID template is correct."""
    expected_template = "bac_san_jose_credit:{{ last_four_digits }}:{{ transaction_id }}"
    assert BacSanJoseCreditExtractor.DEFAULT_IMPORT_ID == expected_template
