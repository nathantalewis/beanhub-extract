import datetime
import decimal
import hashlib
import html.parser
import re
import typing

from ..data_types import Fingerprint
from ..data_types import Transaction
from .base import ExtractorBase


def parse_date(date_str: str) -> datetime.date:
    """Parse DD/MM/YYYY date format used by Banco BCR."""
    parts = date_str.strip().split("/")
    return datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))


class BancoBcrHTMLParser(html.parser.HTMLParser):
    """HTML parser to extract account info and transactions from BCR HTML files."""
    
    def __init__(self):
        super().__init__()
        self.account_holder = None
        self.account_number = None
        self.account_type = None
        self.currency = None
        self.transactions = []
        
        # Parser state
        self._in_account_info = False
        self._in_transaction_table = False
        self._current_row = []
        self._current_cell_data = ""
        self._in_td = False
        
    def handle_starttag(self, tag, attrs):
        if tag == "td":
            self._in_td = True
            self._current_cell_data = ""
        elif tag == "tr":
            self._current_row = []
            
    def handle_endtag(self, tag):
        if tag == "td" and self._in_td:
            self._in_td = False
            self._current_row.append(self._current_cell_data.strip())
        elif tag == "tr" and self._current_row:
            self._process_row()
            
    def handle_data(self, data):
        data = data.strip()
        if not data:
            return
            
        # Look for account holder name (appears before account type info)
        if (not self.account_holder and not self.account_type and 
            len(data) > 5 and data.isupper() and 
            "CUENTA" not in data and "BANCO" not in data and "MOVIMIENTOS" not in data):
            # Account holder names are typically in all caps and contain multiple words
            words = data.split()
            if len(words) >= 2 and all(word.isalpha() for word in words):
                self.account_holder = data
                
        # Look for account type and number
        if "Cuenta Ahorros" in data:
            if "Dólares" in data:
                self.account_type = "Cuenta Ahorros Dólares"
                self.currency = "USD"
            elif "Colones" in data:
                self.account_type = "Cuenta Ahorros Colones" 
                self.currency = "CRC"
                
        # Extract account number from patterns like "CR" + 20 digits
        # Account number might be split across HTML tags, so we collect all CR+digits
        if not self.account_number and data.startswith('CR') and any(c.isdigit() for c in data):
            # Store partial account number, we'll complete it as we parse more data
            if not hasattr(self, '_partial_account'):
                self._partial_account = ""
            self._partial_account += data
            # Check if we have a complete account number (CR + 20 digits)
            cr_match = re.search(r'CR\d{20}', self._partial_account)
            if cr_match:
                self.account_number = cr_match.group()
                
        # Check if we're entering transaction table
        if "Fecha contable" in data and "Fecha transacción" in data:
            self._in_transaction_table = True
            
        if self._in_td:
            self._current_cell_data += data
            
    def _process_row(self):
        """Process a completed table row."""
        if len(self._current_row) == 7:
            # Check if this looks like a transaction row (has dates and amounts)
            fecha_contable = self._current_row[0].strip().replace('&nbsp;', '').strip()
            fecha_transaccion = self._current_row[1].strip().replace('&nbsp;', '').strip()
            
            # Skip header rows and empty rows
            if (fecha_contable and fecha_transaccion and 
                "/" in fecha_contable and "/" in fecha_transaccion and
                not "Fecha" in fecha_contable and
                len(fecha_contable.split("/")) == 3):
                
                self.transactions.append({
                    "fecha_contable": fecha_contable,
                    "fecha_transaccion": fecha_transaccion,
                    "hora": self._current_row[2].strip().replace('&nbsp;', ''),
                    "documento": self._current_row[3].strip().replace('&nbsp;', ''),
                    "descripcion": self._current_row[4].strip().replace('&nbsp;', ''),
                    "debitos": self._current_row[5].strip().replace('&nbsp;', ''),
                    "creditos": self._current_row[6].strip().replace('&nbsp;', ''),
                })


class BancoBcrExtractor(ExtractorBase):
    EXTRACTOR_NAME = "banco_bcr"
    DEFAULT_IMPORT_ID = "banco_bcr:{{ source_account }}:{{ transaction_id }}"

    def detect(self) -> bool:
        """Detect if this is a Banco BCR HTML file."""
        try:
            self.input_file.seek(0)
            content = self.input_file.read()
            self.input_file.seek(0)
            
            # Check for BCR-specific HTML content
            content_lower = content.lower()
            return (
                ("<html>" in content_lower or "<body>" in content_lower) and
                "Banco de Costa Rica" in content and
                "Movimientos de la cuenta" in content and
                "Fecha contable" in content and
                "Fecha transacción" in content
            )
        except Exception:
            return False

    def fingerprint(self) -> Fingerprint | None:
        """Generate fingerprint based on account info and first transaction."""
        try:
            parser = self._parse_html()
            if not parser.account_number or not parser.transactions:
                return None
                
            first_transaction = parser.transactions[0]
            
            # Create hash from account and first transaction
            hash_obj = hashlib.sha256()
            hash_obj.update(parser.account_number.encode("utf8"))
            hash_obj.update(first_transaction["fecha_transaccion"].encode("utf8"))
            hash_obj.update(first_transaction["documento"].encode("utf8"))
            hash_obj.update(first_transaction["descripcion"].encode("utf8"))
            
            return Fingerprint(
                starting_date=parse_date(first_transaction["fecha_transaccion"]),
                first_row_hash=hash_obj.hexdigest(),
            )
        except Exception:
            return None

    def _parse_html(self) -> BancoBcrHTMLParser:
        """Parse the HTML content and return the parser with extracted data."""
        self.input_file.seek(0)
        content = self.input_file.read()
        
        parser = BancoBcrHTMLParser()
        parser.feed(content)
        
        # If account number wasn't found by the parser, try regex on the full content
        if not parser.account_number:
            # Remove HTML tags and look for account number
            import re
            clean_content = re.sub(r'<[^>]+>', '', content)
            cr_match = re.search(r'CR\d{20}', clean_content)
            if cr_match:
                parser.account_number = cr_match.group()
        
        return parser

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from Banco BCR HTML file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        parser = self._parse_html()
        
        if not parser.account_number or not parser.currency:
            return
            
        # Process each transaction
        for i, txn_data in enumerate(parser.transactions):
            try:
                # Parse dates
                transaction_date = parse_date(txn_data["fecha_transaccion"])
                post_date = parse_date(txn_data["fecha_contable"])
                
                # Parse amounts - BCR uses separate debit/credit columns
                debitos = txn_data["debitos"].strip()
                creditos = txn_data["creditos"].strip()
                
                # Determine transaction amount
                amount = None
                if debitos and debitos != "&nbsp;" and debitos != "":
                    # Remove any negative sign and make it negative
                    amount_str = debitos.replace("-", "").replace(",", "")
                    if amount_str and amount_str != "0.00":
                        amount = -decimal.Decimal(amount_str)
                elif creditos and creditos != "&nbsp;" and creditos != "":
                    # Credits are positive
                    amount_str = creditos.replace(",", "")
                    if amount_str and amount_str != "0.00":
                        amount = decimal.Decimal(amount_str)
                        
                if amount is None:
                    continue  # Skip transactions with no amount
                    
                # Create unique transaction_id using reference:datehour (timestamp-like)
                reference = txn_data["documento"]
                hora = txn_data["hora"]
                date_str = transaction_date.strftime("%Y%m%d")
                unique_transaction_id = f"{reference}:{date_str}{hora.replace(':', '')}"
                
                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=i + 1,
                    reversed_lineno=len(parser.transactions) - i,
                    date=transaction_date,
                    post_date=post_date,
                    desc=txn_data["descripcion"],
                    amount=amount,
                    currency=parser.currency,
                    source_account=parser.account_number,
                    reference=reference,
                    transaction_id=unique_transaction_id,
                )
                
            except (ValueError, KeyError, decimal.InvalidOperation, IndexError):
                # Skip malformed transactions but continue processing
                continue
