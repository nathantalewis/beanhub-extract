import datetime
import decimal
import io
import pytest

from beanhub_extract.extractors.wsecu import WSECUExtractor


@pytest.fixture
def wsecu_csv_content():
    return """Account number,Date,Description,Category,Note,Amount,Balance
1234567890-S01,08/15/2024,DIGITAL DEPOSIT FROM SHARE 01,Transfer,,500.00,1250.00
1234567890-S01,08/10/2024,ATM WITHDRAWAL #*123456,Cash,,-200.00,750.00
1234567890-S01,08/05/2024,GROCERY STORE PURCHASE,Groceries,,-45.67,950.00
1234567890-S01,08/01/2024,PAYROLL DEPOSIT,Income,,2000.00,995.67
1234567890-S01,07/28/2024,ONLINE TRANSFER,Transfer,,-300.00,-1004.33
1234567890-S01,07/25/2024,RESTAURANT PURCHASE,Dining,,-25.50,-704.33
1234567890-S01,07/20/2024,CHECK DEPOSIT,Income,,1500.00,-678.83
1234567890-S01,07/15/2024,UTILITY PAYMENT,Bills,,-125.75,-2178.83
1234567890-S01,07/10/2024,BALANCE TRANSFER,Transfer,,1000.00,-2053.08
1234567890-S01,07/05/2024,SERVICE FEE,Fees Charges,,-15.00,-3053.08"""


@pytest.fixture
def invalid_csv_content():
    return """Wrong,Headers,Format
1234567890-S01,08/15/2024,DIGITAL DEPOSIT FROM SHARE 01,Transfer,,500.00,1250.00"""


@pytest.fixture
def empty_csv_content():
    return """Account number,Date,Description,Category,Note,Amount,Balance"""


class TestWSECUExtractor:
    def test_detect_valid_wsecu_csv(self, wsecu_csv_content):
        """Test detection of valid WSECU CSV format."""
        input_file = io.StringIO(wsecu_csv_content)
        extractor = WSECUExtractor(input_file)
        assert extractor.detect() is True

    def test_detect_invalid_csv(self, invalid_csv_content):
        """Test detection fails for invalid CSV format."""
        input_file = io.StringIO(invalid_csv_content)
        extractor = WSECUExtractor(input_file)
        assert extractor.detect() is False

    def test_detect_empty_file(self):
        """Test detection fails for empty file."""
        input_file = io.StringIO("")
        extractor = WSECUExtractor(input_file)
        assert extractor.detect() is False

    def test_fingerprint_valid_csv(self, wsecu_csv_content):
        """Test fingerprint generation for valid CSV."""
        input_file = io.StringIO(wsecu_csv_content)
        extractor = WSECUExtractor(input_file)
        fingerprint = extractor.fingerprint()
        
        assert fingerprint is not None
        assert fingerprint.starting_date == datetime.date(2024, 8, 15)  # First row date (most recent)
        assert isinstance(fingerprint.first_row_hash, str)
        assert len(fingerprint.first_row_hash) == 64  # SHA256 hash length

    def test_fingerprint_empty_csv(self, empty_csv_content):
        """Test fingerprint returns None for empty CSV."""
        input_file = io.StringIO(empty_csv_content)
        extractor = WSECUExtractor(input_file)
        fingerprint = extractor.fingerprint()
        
        assert fingerprint is None

    def test_extract_transactions(self, wsecu_csv_content):
        """Test transaction extraction from WSECU CSV."""
        input_file = io.StringIO(wsecu_csv_content)
        extractor = WSECUExtractor(input_file)
        transactions = list(extractor())
        
        # Should have 10 regular transactions + 1 balance transaction = 11 total
        assert len(transactions) == 11
        
        # Test first regular transaction
        first_txn = transactions[0]
        assert first_txn.extractor == "wsecu"
        assert first_txn.lineno == 1
        assert first_txn.reversed_lineno == -10  # 0 - 10 = -10
        assert first_txn.date == datetime.date(2024, 8, 15)
        assert first_txn.post_date == datetime.date(2024, 8, 15)
        assert first_txn.desc == "DIGITAL DEPOSIT FROM SHARE 01"
        assert first_txn.amount == decimal.Decimal("500.00")
        assert first_txn.category == "Transfer"
        assert first_txn.source_account == "1234567890-S01"
        assert first_txn.extra == {"balance": "1250.00"}

    def test_extract_transaction_with_note(self):
        """Test transaction extraction with note field."""
        csv_content = """Account number,Date,Description,Category,Note,Amount,Balance
1234567890-S01,08/15/2024,TEST TRANSACTION,Transfer,Test note,100.00,500.00"""
        
        input_file = io.StringIO(csv_content)
        extractor = WSECUExtractor(input_file)
        transactions = list(extractor())
        
        # Should have 1 regular transaction + 1 balance transaction = 2 total
        assert len(transactions) == 2
        
        regular_txn = transactions[0]
        assert regular_txn.note == "Test note"

    def test_extract_transaction_without_category(self):
        """Test transaction extraction without category."""
        csv_content = """Account number,Date,Description,Category,Note,Amount,Balance
1234567890-S01,08/15/2024,TEST TRANSACTION,,Test note,100.00,500.00"""
        
        input_file = io.StringIO(csv_content)
        extractor = WSECUExtractor(input_file)
        transactions = list(extractor())
        
        regular_txn = transactions[0]
        assert regular_txn.category is None

    def test_balance_transaction_creation(self, wsecu_csv_content):
        """Test balance transaction is created correctly."""
        input_file = io.StringIO(wsecu_csv_content)
        extractor = WSECUExtractor(input_file)
        transactions = list(extractor())
        
        # Balance transaction should be the last one
        balance_txn = transactions[-1]
        
        assert balance_txn.type == "BALANCE"
        assert balance_txn.date == datetime.date(2024, 8, 16)  # Day after most recent date
        assert balance_txn.post_date == datetime.date(2024, 8, 16)
        assert balance_txn.amount == decimal.Decimal("1250.00")  # Balance from most recent row
        assert balance_txn.desc == "Balance as of 2024-08-15"  # Description shows the transaction date
        assert balance_txn.source_account == "1234567890-S01"
        assert balance_txn.transaction_id == "BALANCE_20240815_1234567890-S01"
        assert balance_txn.lineno == 0  # Balance transaction doesn't correspond to a line
        assert balance_txn.note is None  # Balance transactions should not have notes

    def test_find_most_recent_transaction(self, wsecu_csv_content):
        """Test finding the most recent transaction for balance.
        Since WSECU CSV is ordered with most recent first, should return first row.
        """
        input_file = io.StringIO(wsecu_csv_content)
        extractor = WSECUExtractor(input_file)
        
        most_recent = extractor._find_most_recent_transaction()
        
        assert most_recent is not None
        assert most_recent["Date"] == "08/15/2024"  # First row date (most recent)
        assert most_recent["Balance"] == "1250.00"

    def test_find_most_recent_transaction_with_invalid_first_row(self):
        """Test finding most recent transaction when first row has invalid date."""
        csv_content = """Account number,Date,Description,Category,Note,Amount,Balance
1234567890-S01,invalid-date,INVALID TRANSACTION,Transfer,,100.00,500.00
1234567890-S01,08/15/2024,VALID TRANSACTION,Transfer,,200.00,400.00
1234567890-S01,08/10/2024,ANOTHER TRANSACTION,Cash,,-50.00,450.00"""
        
        input_file = io.StringIO(csv_content)
        extractor = WSECUExtractor(input_file)
        
        most_recent = extractor._find_most_recent_transaction()
        
        assert most_recent is not None
        assert most_recent["Date"] == "08/15/2024"  # First valid row
        assert most_recent["Balance"] == "400.00"

    def test_malformed_transaction_handling(self):
        """Test handling of malformed transactions."""
        csv_content = """Account number,Date,Description,Category,Note,Amount,Balance
1234567890-S01,08/15/2024,VALID TRANSACTION,Transfer,,100.00,500.00
1234567890-S01,invalid-date,INVALID TRANSACTION,Transfer,,invalid-amount,400.00
1234567890-S01,08/10/2024,ANOTHER VALID TRANSACTION,Cash,,-50.00,450.00"""
        
        input_file = io.StringIO(csv_content)
        extractor = WSECUExtractor(input_file)
        transactions = list(extractor())
        
        # Should have 2 valid regular transactions + 1 balance transaction = 3 total
        # (malformed transaction should be skipped)
        assert len(transactions) == 3
        
        # Verify the valid transactions are present
        regular_txns = [t for t in transactions if t.type != "BALANCE"]
        assert len(regular_txns) == 2
        assert regular_txns[0].desc == "VALID TRANSACTION"
        assert regular_txns[1].desc == "ANOTHER VALID TRANSACTION"

    def test_extractor_name_and_import_id(self):
        """Test extractor name and default import ID."""
        assert WSECUExtractor.EXTRACTOR_NAME == "wsecu"
        assert WSECUExtractor.DEFAULT_IMPORT_ID == "wsecu:{{ source_account }}:{{ reversed_lineno }}"

    def test_import_id_format_with_transaction_data(self, wsecu_csv_content):
        """Test that transactions have the correct source_account for import ID generation."""
        input_file = io.StringIO(wsecu_csv_content)
        extractor = WSECUExtractor(input_file)
        transactions = list(extractor())
        
        # Test regular transactions have correct source_account and reversed_lineno
        regular_txns = [t for t in transactions if t.type != "BALANCE"]
        
        first_txn = regular_txns[0]
        assert first_txn.source_account == "1234567890-S01"
        assert first_txn.reversed_lineno == -10
        # The import ID template would resolve to: "wsecu:1234567890-S01:-10"
        
        last_txn = regular_txns[-1]
        assert last_txn.source_account == "1234567890-S01"
        assert last_txn.reversed_lineno == -1  # Last transaction (10th) has reversed_lineno = 9 - 10 = -1
        # The import ID template would resolve to: "wsecu:1234567890-S01:-1"

    def test_all_fields_definition(self):
        """Test that all expected CSV fields are defined."""
        expected_fields = [
            "Account number",
            "Date", 
            "Description",
            "Category",
            "Note",
            "Amount",
            "Balance",
        ]
        assert WSECUExtractor.ALL_FIELDS == expected_fields
