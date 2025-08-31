import datetime
import decimal
import io
import pytest

from beanhub_extract.extractors.banco_bcr import BancoBcrExtractor


@pytest.fixture
def banco_bcr_usd_fixture():
    """Path to USD fixture file."""
    return "tests/extractors/fixtures/banco_bcr_usd.xls"


@pytest.fixture
def banco_bcr_crc_fixture():
    """Path to CRC fixture file."""
    return "tests/extractors/fixtures/banco_bcr_crc.xls"


@pytest.fixture
def invalid_html_content():
    return """<html>
<body>
<h1>Not a BCR file</h1>
<p>This is some other HTML content</p>
</body>
</html>"""


@pytest.fixture
def empty_html_content():
    return """<html><body></body></html>"""


class TestBancoBcrExtractor:
    def test_detect_valid_bcr_usd_fixture(self, banco_bcr_usd_fixture):
        """Test detection of valid BCR USD HTML format using fixture file."""
        with open(banco_bcr_usd_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            assert extractor.detect() is True

    def test_detect_valid_bcr_crc_fixture(self, banco_bcr_crc_fixture):
        """Test detection of valid BCR CRC HTML format using fixture file."""
        with open(banco_bcr_crc_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            assert extractor.detect() is True

    def test_detect_invalid_html(self, invalid_html_content):
        """Test detection fails for invalid HTML format."""
        input_file = io.StringIO(invalid_html_content)
        extractor = BancoBcrExtractor(input_file)
        assert extractor.detect() is False

    def test_detect_empty_file(self):
        """Test detection fails for empty file."""
        input_file = io.StringIO("")
        extractor = BancoBcrExtractor(input_file)
        assert extractor.detect() is False

    def test_detect_non_html_file(self):
        """Test detection fails for non-HTML content."""
        input_file = io.StringIO("This is not HTML content at all")
        extractor = BancoBcrExtractor(input_file)
        assert extractor.detect() is False

    def test_fingerprint_valid_usd_fixture(self, banco_bcr_usd_fixture):
        """Test fingerprint generation for valid USD fixture."""
        with open(banco_bcr_usd_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            fingerprint = extractor.fingerprint()
            
            assert fingerprint is not None
            assert fingerprint.starting_date == datetime.date(2025, 3, 20)  # First transaction date
            assert isinstance(fingerprint.first_row_hash, str)
            assert len(fingerprint.first_row_hash) == 64  # SHA256 hash length

    def test_fingerprint_valid_crc_fixture(self, banco_bcr_crc_fixture):
        """Test fingerprint generation for valid CRC fixture."""
        with open(banco_bcr_crc_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            fingerprint = extractor.fingerprint()
            
            assert fingerprint is not None
            assert fingerprint.starting_date == datetime.date(2025, 3, 1)  # First transaction date
            assert isinstance(fingerprint.first_row_hash, str)
            assert len(fingerprint.first_row_hash) == 64  # SHA256 hash length

    def test_fingerprint_empty_html(self, empty_html_content):
        """Test fingerprint returns None for empty HTML."""
        input_file = io.StringIO(empty_html_content)
        extractor = BancoBcrExtractor(input_file)
        fingerprint = extractor.fingerprint()
        
        assert fingerprint is None

    def test_parse_usd_account_info(self, banco_bcr_usd_fixture):
        """Test parsing USD account information from fixture."""
        with open(banco_bcr_usd_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            parser = extractor._parse_html()
            
            assert parser.account_holder == "TEST USER"
            assert parser.account_number == "CR12345678901234567890"
            assert parser.account_type == "Cuenta Ahorros Dólares"
            assert parser.currency == "USD"

    def test_parse_crc_account_info(self, banco_bcr_crc_fixture):
        """Test parsing CRC account information from fixture."""
        with open(banco_bcr_crc_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            parser = extractor._parse_html()
            
            assert parser.account_holder == "TEST USER"
            assert parser.account_number == "CR98765432109876543210"
            assert parser.account_type == "Cuenta Ahorros Colones"
            assert parser.currency == "CRC"

    def test_extract_usd_transactions(self, banco_bcr_usd_fixture):
        """Test transaction extraction from BCR USD fixture."""
        with open(banco_bcr_usd_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            transactions = list(extractor())
            
            # Should have 3 transactions
            assert len(transactions) == 3
            
            # Test first transaction (debit)
            first_txn = transactions[0]
            assert first_txn.extractor == "banco_bcr"
            assert first_txn.lineno == 1
            assert first_txn.reversed_lineno == 3  # len(transactions) - index = 3 - 0 = 3
            assert first_txn.date == datetime.date(2025, 3, 20)  # Fecha transacción
            assert first_txn.post_date == datetime.date(2025, 3, 20)  # Fecha contable
            assert first_txn.desc == "SERV ADMIN VISA AH"
            assert first_txn.amount == decimal.Decimal("-1.00")
            assert first_txn.currency == "USD"
            assert first_txn.source_account == "CR12345678901234567890"
            assert first_txn.transaction_id == "1234567:202503200640"
            assert first_txn.reference == "1234567"
            
            # Test second transaction (debit with different post date)
            second_txn = transactions[1]
            assert second_txn.date == datetime.date(2025, 3, 24)  # Fecha transacción
            assert second_txn.post_date == datetime.date(2025, 3, 23)  # Fecha contable
            assert second_txn.desc == "DB AH QUICK PASS/BILL ELECT"
            assert second_txn.amount == decimal.Decimal("-16.14")
            assert second_txn.transaction_id == "7654321:202503241422"
            assert second_txn.reference == "7654321"
            
            # Test third transaction (credit)
            third_txn = transactions[2]
            assert third_txn.date == datetime.date(2025, 3, 25)
            assert third_txn.desc == "DEPOSIT"
            assert third_txn.amount == decimal.Decimal("500.00")
            assert third_txn.transaction_id == "9876543:202503250915"
            assert third_txn.reference == "9876543"

    def test_extract_crc_transactions(self, banco_bcr_crc_fixture):
        """Test transaction extraction from BCR CRC fixture."""
        with open(banco_bcr_crc_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            transactions = list(extractor())
            
            # Should have 3 transactions
            assert len(transactions) == 3
            
            # Test first transaction (credit)
            first_txn = transactions[0]
            assert first_txn.extractor == "banco_bcr"
            assert first_txn.date == datetime.date(2025, 3, 1)
            assert first_txn.desc == "TRANSFERENC BANCOBCR/TARJETA"
            assert first_txn.amount == decimal.Decimal("50000.00")
            assert first_txn.currency == "CRC"
            assert first_txn.source_account == "CR98765432109876543210"
            assert first_txn.transaction_id == "1111111:202503010830"
            assert first_txn.reference == "1111111"
            
            # Test second transaction (debit)
            second_txn = transactions[1]
            assert second_txn.date == datetime.date(2025, 3, 15)
            assert second_txn.desc == "SINPE MOVIL OTRA ENT/Test Payment"
            assert second_txn.amount == decimal.Decimal("-25000.00")
            assert second_txn.transaction_id == "2222222:202503151045"
            assert second_txn.reference == "2222222"
            
            # Test third transaction (interest)
            third_txn = transactions[2]
            assert third_txn.date == datetime.date(2025, 3, 31)
            assert third_txn.desc == "INTS GANADOS AHORROS"
            assert third_txn.amount == decimal.Decimal("1250.00")
            assert third_txn.transaction_id == "3333333:202503312359"
            assert third_txn.reference == "3333333"

    def test_different_post_and_transaction_dates(self):
        """Test handling of different post and transaction dates."""
        html_content = """<html><body>
		<table><tr><th colspan="7">Banco de Costa Rica</th></tr>
		<tr><th colspan="7">Movimientos de la cuenta</th></tr>
		<tr><th colspan="7">TEST USER</th></tr>
		<tr><th colspan="7">Cuenta Ahorros Dólares : CR12345678901234567890</th></tr></table>
		<table id="t1">
			<tr><th>Fecha contable</th><th>Fecha transacción</th><th>Hora</th><th>Documento</th><th>Descripción</th><th>Débitos</th><th>Créditos</th></tr>
			<tr>
				<td>18/01/2025</td>
				<td>17/01/2025</td>
				<td>23:59</td>
				<td>1111111</td>
				<td>LATE TRANSACTION</td>
				<td>50.00</td>
				<td>&nbsp;</td>
			</tr>
		</table></body></html>"""
        
        input_file = io.StringIO(html_content)
        extractor = BancoBcrExtractor(input_file)
        transactions = list(extractor())
        
        assert len(transactions) == 1
        txn = transactions[0]
        assert txn.date == datetime.date(2025, 1, 17)  # Fecha transacción
        assert txn.post_date == datetime.date(2025, 1, 18)  # Fecha contable

    def test_malformed_transaction_handling(self):
        """Test handling of malformed transactions."""
        html_content = """<html><body>
		<table><tr><th colspan="7">Banco de Costa Rica</th></tr>
		<tr><th colspan="7">Movimientos de la cuenta</th></tr>
		<tr><th colspan="7">TEST USER</th></tr>
		<tr><th colspan="7">Cuenta Ahorros Dólares : CR12345678901234567890</th></tr></table>
		<table id="t1">
			<tr><th>Fecha contable</th><th>Fecha transacción</th><th>Hora</th><th>Documento</th><th>Descripción</th><th>Débitos</th><th>Créditos</th></tr>
			<tr>
				<td>15/01/2025</td>
				<td>15/01/2025</td>
				<td>10:30</td>
				<td>1234567</td>
				<td>VALID TRANSACTION</td>
				<td>100.00</td>
				<td>&nbsp;</td>
			</tr>
			<tr>
				<td>invalid-date</td>
				<td>invalid-date</td>
				<td>10:30</td>
				<td>9999999</td>
				<td>INVALID TRANSACTION</td>
				<td>invalid-amount</td>
				<td>&nbsp;</td>
			</tr>
			<tr>
				<td>16/01/2025</td>
				<td>16/01/2025</td>
				<td>14:15</td>
				<td>7654321</td>
				<td>ANOTHER VALID TRANSACTION</td>
				<td>&nbsp;</td>
				<td>500.00</td>
			</tr>
		</table></body></html>"""
        
        input_file = io.StringIO(html_content)
        extractor = BancoBcrExtractor(input_file)
        transactions = list(extractor())
        
        # Should have 2 valid transactions (malformed one should be skipped)
        assert len(transactions) == 2
        assert transactions[0].desc == "VALID TRANSACTION"
        assert transactions[1].desc == "ANOTHER VALID TRANSACTION"

    def test_transaction_with_no_amount(self):
        """Test handling of transactions with no amount (should be skipped)."""
        html_content = """<html><body>
		<table><tr><th colspan="7">Banco de Costa Rica</th></tr>
		<tr><th colspan="7">Movimientos de la cuenta</th></tr>
		<tr><th colspan="7">TEST USER</th></tr>
		<tr><th colspan="7">Cuenta Ahorros Dólares : CR12345678901234567890</th></tr></table>
		<table id="t1">
			<tr><th>Fecha contable</th><th>Fecha transacción</th><th>Hora</th><th>Documento</th><th>Descripción</th><th>Débitos</th><th>Créditos</th></tr>
			<tr>
				<td>15/01/2025</td>
				<td>15/01/2025</td>
				<td>10:30</td>
				<td>1234567</td>
				<td>TRANSACTION WITH AMOUNT</td>
				<td>100.00</td>
				<td>&nbsp;</td>
			</tr>
			<tr>
				<td>16/01/2025</td>
				<td>16/01/2025</td>
				<td>14:15</td>
				<td>7654321</td>
				<td>TRANSACTION WITH NO AMOUNT</td>
				<td>&nbsp;</td>
				<td>&nbsp;</td>
			</tr>
		</table></body></html>"""
        
        input_file = io.StringIO(html_content)
        extractor = BancoBcrExtractor(input_file)
        transactions = list(extractor())
        
        # Should have 1 transaction (the one with no amount should be skipped)
        assert len(transactions) == 1
        assert transactions[0].desc == "TRANSACTION WITH AMOUNT"

    def test_extractor_name_and_import_id(self):
        """Test extractor name and default import ID."""
        assert BancoBcrExtractor.EXTRACTOR_NAME == "banco_bcr"
        assert BancoBcrExtractor.DEFAULT_IMPORT_ID == "banco_bcr:{{ source_account }}:{{ transaction_id }}"

    def test_import_id_format_with_transaction_data(self, banco_bcr_usd_fixture):
        """Test that transactions have the correct fields for import ID generation."""
        with open(banco_bcr_usd_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            transactions = list(extractor())
            
            first_txn = transactions[0]
            assert first_txn.source_account == "CR12345678901234567890"
            assert first_txn.transaction_id == "1234567:202503200640"
            assert first_txn.reference == "1234567"
            # The import ID template would resolve to: "banco_bcr:CR12345678901234567890:1234567:202503200640"
            
            second_txn = transactions[1]
            assert second_txn.source_account == "CR12345678901234567890"
            assert second_txn.transaction_id == "7654321:202503241422"
            assert second_txn.reference == "7654321"
            # The import ID template would resolve to: "banco_bcr:CR12345678901234567890:7654321:202503241422"

    def test_account_number_with_html_tags(self):
        """Test account number extraction when split across HTML tags."""
        html_content = """<html><body>
		<table><tr><th colspan="7">Banco de Costa Rica</th></tr>
		<tr><th colspan="7">Movimientos de la cuenta</th></tr>
		<tr><th colspan="7">TEST USER</th></tr>
		<tr><th colspan="7">Cuenta Ahorros Dólares : CR12345678<font>90123456</font>7890</th></tr></table>
		<table id="t1">
			<tr><th>Fecha contable</th><th>Fecha transacción</th><th>Hora</th><th>Documento</th><th>Descripción</th><th>Débitos</th><th>Créditos</th></tr>
			<tr>
				<td>15/01/2025</td>
				<td>15/01/2025</td>
				<td>10:30</td>
				<td>1234567</td>
				<td>TEST TRANSACTION</td>
				<td>100.00</td>
				<td>&nbsp;</td>
			</tr>
		</table></body></html>"""
        
        input_file = io.StringIO(html_content)
        extractor = BancoBcrExtractor(input_file)
        parser = extractor._parse_html()
        
        # Should extract the complete account number despite HTML tags
        assert parser.account_number == "CR12345678901234567890"

    def test_nbsp_handling(self):
        """Test handling of &nbsp; entities in transaction data."""
        html_content = """<html><body>
		<table><tr><th colspan="7">Banco de Costa Rica</th></tr>
		<tr><th colspan="7">Movimientos de la cuenta</th></tr>
		<tr><th colspan="7">TEST USER</th></tr>
		<tr><th colspan="7">Cuenta Ahorros Dólares : CR12345678901234567890</th></tr></table>
		<table id="t1">
			<tr><th>Fecha contable</th><th>Fecha transacción</th><th>Hora</th><th>Documento</th><th>Descripción</th><th>Débitos</th><th>Créditos</th></tr>
			<tr>
				<td>&nbsp;15/01/2025</td>
				<td>&nbsp;15/01/2025</td>
				<td>10:30</td>
				<td>1234567</td>
				<td>TEST TRANSACTION</td>
				<td>100.00</td>
				<td>&nbsp;</td>
			</tr>
		</table></body></html>"""
        
        input_file = io.StringIO(html_content)
        extractor = BancoBcrExtractor(input_file)
        transactions = list(extractor())
        
        assert len(transactions) == 1
        txn = transactions[0]
        assert txn.date == datetime.date(2025, 1, 15)  # Should parse correctly despite &nbsp;

    def test_usd_vs_crc_currency_detection(self, banco_bcr_usd_fixture, banco_bcr_crc_fixture):
        """Test that USD and CRC currencies are correctly detected."""
        # Test USD fixture
        with open(banco_bcr_usd_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            transactions = list(extractor())
            assert all(txn.currency == "USD" for txn in transactions)
        
        # Test CRC fixture
        with open(banco_bcr_crc_fixture, 'r', encoding='utf-8') as f:
            extractor = BancoBcrExtractor(f)
            transactions = list(extractor())
            assert all(txn.currency == "CRC" for txn in transactions)

    def test_fixture_files_exist_and_are_valid(self, banco_bcr_usd_fixture, banco_bcr_crc_fixture):
        """Test that fixture files exist and contain valid BCR data."""
        # Test USD fixture exists and is valid
        with open(banco_bcr_usd_fixture, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "Banco de Costa Rica" in content
            assert "Cuenta Ahorros Dólares" in content
            assert "CR12345678901234567890" in content
        
        # Test CRC fixture exists and is valid
        with open(banco_bcr_crc_fixture, 'r', encoding='utf-8') as f:
            content = f.read()
            assert "Banco de Costa Rica" in content
            assert "Cuenta Ahorros Colones" in content
            assert "CR98765432109876543210" in content
