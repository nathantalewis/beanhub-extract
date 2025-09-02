import datetime
import pathlib

import pytest

from beanhub_extract.extractors.bac_san_jose_bank import BacSanJoseBankExtractor, parse_date


@pytest.mark.parametrize(
    "date_str, expected",
    [
        ("01/03/2025", datetime.date(2025, 3, 1)),
        ("31/12/2024", datetime.date(2024, 12, 31)),
        ("15/06/2025", datetime.date(2025, 6, 15)),
    ],
)
def test_parse_date(date_str: str, expected: datetime.date):
    assert parse_date(date_str) == expected


def test_bac_san_jose_bank_extractor(fixtures_folder: pathlib.Path):
    """Test that the BAC San José bank extractor can parse transactions correctly."""
    input_file_path = fixtures_folder / "bac_san_jose_bank.csv"
    with input_file_path.open("rt", encoding="utf-8") as fo:
        extractor = BacSanJoseBankExtractor(fo)
        result = list(extractor())
        
        # Should have 6 transactions (5 regular + 1 balance)
        assert len(result) == 6
        
        # Check that we have the expected transaction types
        regular_transactions = [txn for txn in result if txn.type != "BALANCE"]
        balance_transactions = [txn for txn in result if txn.type == "BALANCE"]
        
        assert len(regular_transactions) == 5
        assert len(balance_transactions) == 1
        
        # Check some key properties
        assert all(txn.extractor == "bac_san_jose_bank" for txn in result)
        assert all(txn.currency == "CRC" for txn in result)
        assert all(txn.source_account == "CR12345678901234567890" for txn in result)
        
        # Check specific transaction descriptions
        descriptions = {txn.desc for txn in regular_transactions}
        assert "TRANSFERENCIA RECIBIDA" in descriptions
        assert "DEPOSITO EFECTIVO" in descriptions
        assert "PAGO SERVICIOS" in descriptions


def test_bac_san_jose_bank_fingerprint(fixtures_folder: pathlib.Path):
    """Test that the BAC San José bank extractor generates a valid fingerprint."""
    input_file_path = fixtures_folder / "bac_san_jose_bank.csv"
    with input_file_path.open("rt", encoding="utf-8") as fo:
        extractor = BacSanJoseBankExtractor(fo)
        result = extractor.fingerprint()
        
        # We can't predict the exact hash, so just verify the structure
        assert result is not None
        assert result.starting_date == datetime.date(2025, 3, 20)
        assert len(result.first_row_hash) == 64  # SHA256 hash length


@pytest.mark.parametrize(
    "input_file, expected",
    [
        ("bac_san_jose_bank.csv", True),
        ("bac_san_jose_credit.csv", False),
        ("csv.csv", False),
        ("empty.csv", False),
    ],
)
def test_bac_san_jose_bank_detect(
    input_file: str, expected: bool, fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rt", encoding="utf-8") as fo:
        extractor = BacSanJoseBankExtractor(fo)
        assert extractor.detect() == expected


def test_bac_san_jose_bank_extractor_name():
    """Test that the extractor has the correct name."""
    assert BacSanJoseBankExtractor.EXTRACTOR_NAME == "bac_san_jose_bank"


def test_bac_san_jose_bank_import_id_template():
    """Test that the import ID template is correct."""
    expected_template = "bac_san_jose:{{ source_account }}:{{ reference }}"
    assert BacSanJoseBankExtractor.DEFAULT_IMPORT_ID == expected_template
