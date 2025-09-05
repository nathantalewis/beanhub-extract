import datetime
import decimal
import pathlib

import pytest

from beanhub_extract.data_types import Transaction
from beanhub_extract.extractors.synchrony_pdf import SynchronyPdfExtractor
from beanhub_extract.utils import strip_txn_base_path


@pytest.mark.parametrize(
    "input_file, expected",
    [
        (
            "synchrony.pdf",
            [
                Transaction(
                    extractor="synchrony_pdf",
                    file="synchrony.pdf",
                    lineno=1,
                    reversed_lineno=-3,
                    date=datetime.date(2024, 3, 15),
                    desc="INTEREST ADDED",
                    amount=decimal.Decimal("50.25"),
                    currency="USD",
                    source_account="9999",
                ),
                Transaction(
                    extractor="synchrony_pdf",
                    file="synchrony.pdf",
                    lineno=2,
                    reversed_lineno=-2,
                    date=datetime.date(2024, 2, 15),
                    desc="INTEREST ADDED",
                    amount=decimal.Decimal("48.75"),
                    currency="USD",
                    source_account="9999",
                ),
                Transaction(
                    extractor="synchrony_pdf",
                    file="synchrony.pdf",
                    lineno=3,
                    reversed_lineno=-1,
                    date=datetime.date(2024, 1, 15),
                    desc="DEPOSIT",
                    amount=decimal.Decimal("10000.00"),
                    currency="USD",
                    source_account="9999",
                ),
                Transaction(
                    extractor="synchrony_pdf",
                    file="synchrony.pdf",
                    lineno=4,
                    reversed_lineno=0,
                    transaction_id="BALANCE_2024-03-16",
                    date=datetime.date(2024, 3, 16),
                    desc="Balance as of 2024-03-16",
                    amount=decimal.Decimal("10099.00"),
                    currency="USD",
                    type="BALANCE",
                    source_account="9999",
                ),
            ],
        ),
    ],
)
def test_synchrony_pdf_extractor(
    input_file: str, expected: list[Transaction], fixtures_folder: pathlib.Path
):
    input_file_path = fixtures_folder / input_file
    with input_file_path.open("rb") as fo:
        extractor = SynchronyPdfExtractor(fo)
        result = list(extractor())
        result = [strip_txn_base_path(fixtures_folder, txn) for txn in result]
        assert result == expected


def test_synchrony_pdf_detect(fixtures_folder: pathlib.Path):
    """Test detection of Synchrony PDF files"""
    input_file_path = fixtures_folder / "synchrony.pdf"
    with input_file_path.open("rb") as fo:
        extractor = SynchronyPdfExtractor(fo)
        assert extractor.detect() is True


def test_synchrony_pdf_fingerprint(fixtures_folder: pathlib.Path):
    """Test fingerprint generation for Synchrony PDF files"""
    input_file_path = fixtures_folder / "synchrony.pdf"
    with input_file_path.open("rb") as fo:
        extractor = SynchronyPdfExtractor(fo)
        fingerprint = extractor.fingerprint()
        assert fingerprint is not None
        assert fingerprint.starting_date == datetime.date(2024, 1, 15)  # Earliest transaction
        assert len(fingerprint.first_row_hash) == 16
