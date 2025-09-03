import datetime
import decimal
import io
import pytest

from beanhub_extract.extractors.banco_nacional import BancoNacionalExtractor


@pytest.fixture
def banco_nacional_fixture():
    """Path to Banco Nacional fixture file."""
    return "tests/extractors/fixtures/banco_nacional.csv"


@pytest.fixture
def invalid_csv_content():
    return """header1,header2,header3
value1,value2,value3
another,row,here"""


@pytest.fixture
def empty_csv_content():
    return ""


@pytest.fixture
def malformed_csv_content():
    return """oficina;fechaMovimiento;numeroDocumento;debito;credito;descripcion;
001;invalid-date;2001001;;1.25;INTERESES GANADOS;
001;01/08/2025;2001002;;invalid-amount;ANOTHER TRANSACTION;
001;01/07/2025;2001003;;1.25;VALID TRANSACTION;"""


class TestBancoNacionalExtractor:
    def test_detect_valid_banco_nacional_fixture(self, banco_nacional_fixture):
        """Test detection of valid Banco Nacional CSV format using fixture file."""
        with open(banco_nacional_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoNacionalExtractor(f)
            assert extractor.detect() is True

    def test_detect_invalid_csv(self, invalid_csv_content):
        """Test detection fails for invalid CSV format."""
        input_file = io.StringIO(invalid_csv_content)
        extractor = BancoNacionalExtractor(input_file)
        assert extractor.detect() is False

    def test_detect_empty_file(self):
        """Test detection fails for empty file."""
        input_file = io.StringIO("")
        extractor = BancoNacionalExtractor(input_file)
        assert extractor.detect() is False

    def test_detect_non_semicolon_csv(self):
        """Test detection fails for comma-separated CSV."""
        csv_content = """oficina,fechaMovimiento,numeroDocumento,debito,credito,descripcion
001,15/12/2024,2001001,,1.25,INTERESES GANADOS"""
        input_file = io.StringIO(csv_content)
        extractor = BancoNacionalExtractor(input_file)
        assert extractor.detect() is False

    def test_fingerprint_valid_fixture(self, banco_nacional_fixture):
        """Test fingerprint generation for valid fixture."""
        with open(banco_nacional_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoNacionalExtractor(f)
            fingerprint = extractor.fingerprint()
            
            assert fingerprint is not None
            assert fingerprint.starting_date == datetime.date(2024, 12, 15)  # First transaction date
            assert isinstance(fingerprint.first_row_hash, str)
            assert len(fingerprint.first_row_hash) == 64  # SHA256 hash length

    def test_fingerprint_empty_csv(self, empty_csv_content):
        """Test fingerprint returns None for empty CSV."""
        input_file = io.StringIO(empty_csv_content)
        extractor = BancoNacionalExtractor(input_file)
        fingerprint = extractor.fingerprint()
        
        assert fingerprint is None

    def test_extract_transactions(self, banco_nacional_fixture):
        """Test transaction extraction from Banco Nacional fixture."""
        with open(banco_nacional_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoNacionalExtractor(f)
            transactions = list(extractor())
            
            # Should have 8 transactions (excluding the TOTAL row)
            assert len(transactions) == 8
            
            # Test first transaction (credit - interest)
            first_txn = transactions[0]
            assert first_txn.extractor == "banco_nacional"
            assert first_txn.lineno == 2  # First data row after header
            assert first_txn.reversed_lineno == 8  # len(transactions) - index = 8 - 0 = 8
            assert first_txn.date == datetime.date(2024, 12, 15)
            assert first_txn.post_date == datetime.date(2024, 12, 15)
            assert first_txn.desc == "INTERESES GANADOS EN SU CUENTA DE AHORRO/BNCR"
            assert first_txn.amount == decimal.Decimal("1.25")
            assert first_txn.currency is None
            assert first_txn.source_account is None
            assert first_txn.transaction_id == "2001001"
            assert first_txn.reference == "2001001"
            assert first_txn.extra["oficina"] == "001"
            
            # Test a debit transaction
            debit_txn = transactions[3]  # ATM withdrawal
            assert debit_txn.date == datetime.date(2024, 9, 20)
            assert debit_txn.desc == "RETIRO ATM OFICINA PRINCIPAL"
            assert debit_txn.amount == decimal.Decimal("-75.00")
            assert debit_txn.transaction_id == "2001004"
            assert debit_txn.reference == "2001004"
            
            # Test another credit transaction
            credit_txn = transactions[4]  # Cash deposit
            assert credit_txn.date == datetime.date(2024, 9, 18)
            assert credit_txn.desc == "DEPOSITO EN EFECTIVO"
            assert credit_txn.amount == decimal.Decimal("250.00")
            assert credit_txn.transaction_id == "2001005"
            assert credit_txn.reference == "2001005"
            
            # Test POS purchase (debit)
            pos_txn = transactions[5]
            assert pos_txn.date == datetime.date(2024, 9, 12)
            assert pos_txn.desc == "COMPRA POS COMERCIO LOCAL"
            assert pos_txn.amount == decimal.Decimal("-45.75")
            assert pos_txn.transaction_id == "2001006"
            assert pos_txn.reference == "2001006"

    def test_malformed_transaction_handling(self, malformed_csv_content):
        """Test handling of malformed transactions."""
        input_file = io.StringIO(malformed_csv_content)
        extractor = BancoNacionalExtractor(input_file)
        transactions = list(extractor())
        
        # Should have 1 valid transaction (malformed ones should be skipped)
        assert len(transactions) == 1
        assert transactions[0].desc == "VALID TRANSACTION"
        assert transactions[0].amount == decimal.Decimal("1.25")

    def test_transaction_with_no_amount(self):
        """Test handling of transactions with no amount (should be skipped)."""
        csv_content = """oficina;fechaMovimiento;numeroDocumento;debito;credito;descripcion;
001;15/12/2024;2001001;;1.25;TRANSACTION WITH AMOUNT;
001;15/11/2024;2001002;;;TRANSACTION WITH NO AMOUNT;
001;15/10/2024;2001003;0.00;;TRANSACTION WITH ZERO DEBIT;"""
        
        input_file = io.StringIO(csv_content)
        extractor = BancoNacionalExtractor(input_file)
        transactions = list(extractor())
        
        # Should have 1 transaction (the ones with no amount should be skipped)
        assert len(transactions) == 1
        assert transactions[0].desc == "TRANSACTION WITH AMOUNT"

    def test_total_row_skipping(self):
        """Test that TOTAL rows are properly skipped."""
        csv_content = """oficina;fechaMovimiento;numeroDocumento;debito;credito;descripcion;
001;01/09/2025;2001001;;1.25;VALID TRANSACTION;
;;TOTAL;50.00;100.00;;
001;01/08/2025;2001002;25.00;;ANOTHER VALID TRANSACTION;"""
        
        input_file = io.StringIO(csv_content)
        extractor = BancoNacionalExtractor(input_file)
        transactions = list(extractor())
        
        # Should have 2 transactions (TOTAL row should be skipped)
        assert len(transactions) == 2
        assert transactions[0].desc == "VALID TRANSACTION"
        assert transactions[1].desc == "ANOTHER VALID TRANSACTION"

    def test_empty_date_row_skipping(self):
        """Test that rows with empty dates are properly skipped."""
        csv_content = """oficina;fechaMovimiento;numeroDocumento;debito;credito;descripcion;
001;01/09/2025;2001001;;1.25;VALID TRANSACTION;
001;;2001002;25.00;;TRANSACTION WITH EMPTY DATE;
001;01/08/2025;9999999;10.00;;ANOTHER VALID TRANSACTION;"""
        
        input_file = io.StringIO(csv_content)
        extractor = BancoNacionalExtractor(input_file)
        transactions = list(extractor())
        
        # Should have 2 transactions (empty date row should be skipped)
        assert len(transactions) == 2
        assert transactions[0].desc == "VALID TRANSACTION"
        assert transactions[1].desc == "ANOTHER VALID TRANSACTION"

    def test_extractor_name_and_import_id(self):
        """Test extractor name and default import ID."""
        assert BancoNacionalExtractor.EXTRACTOR_NAME == "banco_nacional"
        assert BancoNacionalExtractor.DEFAULT_IMPORT_ID == "banco_nacional:{{ transaction_id }}"

    def test_import_id_format_with_transaction_data(self, banco_nacional_fixture):
        """Test that transactions have the correct fields for import ID generation."""
        with open(banco_nacional_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoNacionalExtractor(f)
            transactions = list(extractor())
            
            first_txn = transactions[0]
            assert first_txn.source_account is None
            assert first_txn.transaction_id == "2001001"
            assert first_txn.reference == "2001001"
            assert first_txn.extra["oficina"] == "001"
            # The import ID template would resolve to: "banco_nacional:2001001"

    def test_currency_is_none(self, banco_nacional_fixture):
        """Test that currency is None since it cannot be determined from the file."""
        with open(banco_nacional_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoNacionalExtractor(f)
            transactions = list(extractor())
            
            assert all(txn.currency is None for txn in transactions)

    def test_date_parsing(self):
        """Test date parsing with DD/MM/YYYY format."""
        csv_content = """oficina;fechaMovimiento;numeroDocumento;debito;credito;descripcion;
001;15/12/2024;2001001;;1.25;TEST TRANSACTION;
001;01/01/2025;2001002;25.00;;NEW YEAR TRANSACTION;"""
        
        input_file = io.StringIO(csv_content)
        extractor = BancoNacionalExtractor(input_file)
        transactions = list(extractor())
        
        assert len(transactions) == 2
        assert transactions[0].date == datetime.date(2024, 12, 15)
        assert transactions[1].date == datetime.date(2025, 1, 1)

    def test_fixture_file_exists_and_is_valid(self, banco_nacional_fixture):
        """Test that fixture file exists and contains valid Banco Nacional data."""
        with open(banco_nacional_fixture, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "oficina;fechaMovimiento;numeroDocumento;debito;credito;descripcion;" in content
            assert "INTERESES GANADOS" in content
            assert "001;" in content

    def test_semicolon_delimiter_detection(self):
        """Test that the extractor specifically detects semicolon-delimited files."""
        # Valid semicolon-delimited file
        valid_content = """oficina;fechaMovimiento;numeroDocumento;debito;credito;descripcion;
001;01/09/2025;2001001;;1.25;TEST;"""
        
        # Invalid comma-delimited file with same headers
        invalid_content = """oficina,fechaMovimiento,numeroDocumento,debito,credito,descripcion
119,01/09/2025,2001001,,1.25,TEST"""
        
        valid_file = io.StringIO(valid_content)
        invalid_file = io.StringIO(invalid_content)
        
        valid_extractor = BancoNacionalExtractor(valid_file)
        invalid_extractor = BancoNacionalExtractor(invalid_file)
        
        assert valid_extractor.detect() is True
        assert invalid_extractor.detect() is False

    def test_whitespace_handling(self):
        """Test handling of whitespace in CSV data."""
        csv_content = """oficina;fechaMovimiento;numeroDocumento;debito;credito;descripcion;
 119 ; 01/09/2025 ; 2001001 ;; 1.25 ; TEST TRANSACTION ;
001;01/08/2025;2001002;25.00;;  ANOTHER TEST  ;"""
        
        input_file = io.StringIO(csv_content)
        extractor = BancoNacionalExtractor(input_file)
        transactions = list(extractor())
        
        assert len(transactions) == 2
        # Whitespace should be stripped
        assert transactions[0].source_account is None
        assert transactions[0].reference == "2001001"
        assert transactions[0].desc == "TEST TRANSACTION"
        assert transactions[1].desc == "ANOTHER TEST"
