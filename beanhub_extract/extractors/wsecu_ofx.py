import datetime
import decimal
import hashlib
import typing

from ofxtools.Parser import OFXTree

from ..data_types import Fingerprint
from ..data_types import Transaction
from .base import ExtractorBase


def parse_ofx_datetime(dt_str: str) -> datetime.date:
    """Parse OFX datetime format (YYYYMMDDHHMMSS.sss) to date."""
    # OFX datetime format is YYYYMMDDHHMMSS.sss, we only need the date part
    date_part = dt_str[:8]  # YYYYMMDD
    return datetime.datetime.strptime(date_part, "%Y%m%d").date()


class WsecuOFXExtractor(ExtractorBase):
    EXTRACTOR_NAME = "wsecu_ofx"
    DEFAULT_IMPORT_ID = "wsecu:{{ source_account }}:{{ transaction_id }}"

    def detect(self) -> bool:
        """Detect if this is a WSECU OFX file."""
        try:
            self.input_file.seek(0)
            content = self.input_file.read(1000)  # Read first 1000 chars
            self.input_file.seek(0)
            
            # Convert to string if it's bytes
            if isinstance(content, bytes):
                content = content.decode('utf-8', errors='ignore')
            
            # Check for OFX header and WSECU specific markers
            # XML format uses OFXHEADER="200", SGML uses OFXHEADER:
            has_ofx_header = "OFXHEADER:" in content or 'OFXHEADER="' in content
            return (
                has_ofx_header and
                "<OFX>" in content and
                "<BANKID>325181028" in content
            )
        except Exception:
            return False

    def fingerprint(self) -> Fingerprint | None:
        """Generate fingerprint based on first transaction."""
        try:
            transactions = list(self._parse_transactions())
            if not transactions:
                return None
            
            # Use first transaction for fingerprint
            first_txn = transactions[0]
            
            # Create hash from first transaction details
            hash_obj = hashlib.sha256()
            hash_obj.update(str(first_txn.get('FITID', '')).encode('utf-8'))
            hash_obj.update(str(first_txn.get('TRNAMT', '')).encode('utf-8'))
            hash_obj.update(str(first_txn.get('NAME', '')).encode('utf-8'))
            
            return Fingerprint(
                starting_date=parse_ofx_datetime(first_txn['DTPOSTED']),
                first_row_hash=hash_obj.hexdigest(),
            )
        except Exception:
            return None

    def _parse_transactions(self) -> typing.Generator[dict, None, None]:
        """Parse OFX file and yield transaction dictionaries."""
        self.input_file.seek(0)
        
        try:
            # ofxtools requires binary mode, so always convert text to binary
            content = self.input_file.read()
            self.input_file.seek(0)
            
            # Preprocess: Remove " GMT" suffix from datetime fields as ofxtools can't handle it
            # This handles XML format OFX files that include timezone suffixes
            if isinstance(content, bytes):
                content = content.decode('utf-8', errors='ignore')
            content = content.replace(' GMT</DTSERVER>', '</DTSERVER>')
            content = content.replace(' GMT</DTPOSTED>', '</DTPOSTED>')
            content = content.replace(' GMT</DTASOF>', '</DTASOF>')
            content = content.replace(' GMT</DTSTART>', '</DTSTART>')
            content = content.replace(' GMT</DTEND>', '</DTEND>')
            
            from io import BytesIO
            binary_file = BytesIO(content.encode('utf-8'))
            
            parser = OFXTree()
            parser.parse(binary_file)
            ofx = parser.convert()
            
            if not hasattr(ofx, 'statements') or not ofx.statements:
                return
            
            for stmt in ofx.statements:
                # Extract statement metadata
                currency = stmt.curdef if hasattr(stmt, 'curdef') else 'USD'
                
                # Get account information
                if not hasattr(stmt, 'bankacctfrom'):
                    raise ValueError("OFX statement missing bankacctfrom - cannot extract account information")
                
                account_id = stmt.bankacctfrom.acctid
                if not account_id:
                    raise ValueError("OFX statement bankacctfrom missing acctid - cannot identify account")
                
                # Get balance information
                ledger_balance = None
                ledger_date = None
                if hasattr(stmt, 'ledgerbal') and hasattr(stmt.ledgerbal, 'balamt'):
                    ledger_balance = str(stmt.ledgerbal.balamt)
                    ledger_date = stmt.ledgerbal.dtasof.strftime('%Y%m%d%H%M%S')
                
                # Extract transactions using the correct approach
                if hasattr(stmt, 'banktranlist') and stmt.banktranlist:
                    # Iterate over banktranlist directly to get transactions
                    transactions = list(stmt.banktranlist)
                    
                    for txn in transactions:
                        # Extract transaction data
                        txn_data = {
                            'TRNTYPE': txn.trntype,
                            'DTPOSTED': txn.dtposted.strftime('%Y%m%d%H%M%S'),
                            'TRNAMT': str(txn.trnamt),
                            'FITID': txn.fitid,
                            'NAME': txn.name,
                        }
                        
                        # Add memo if present
                        if hasattr(txn, 'memo') and txn.memo:
                            txn_data['MEMO'] = txn.memo
                        
                        # Add metadata
                        txn_data['CURRENCY'] = currency
                        txn_data['ACCOUNT_ID'] = account_id
                        
                        yield txn_data
                    
                    # Yield balance transaction if available
                    if ledger_balance and ledger_date:
                        yield {
                            'TRNTYPE': 'BALANCE',
                            'DTPOSTED': ledger_date,
                            'TRNAMT': ledger_balance,
                            'FITID': f"BALANCE_{ledger_date}",
                            'NAME': f"Balance as of {parse_ofx_datetime(ledger_date)}",
                            'CURRENCY': currency,
                            'ACCOUNT_ID': account_id,
                        }
                        
        except Exception as e:
            # Log error but don't crash
            print(f"Error parsing WSECU OFX file: {e}")
            return

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions and convert to Transaction objects."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name
            
        for lineno, txn_data in enumerate(self._parse_transactions(), 1):
            try:
                # Validate required transaction fields
                if 'DTPOSTED' not in txn_data:
                    raise ValueError(f"Transaction {lineno} missing DTPOSTED")
                if 'TRNAMT' not in txn_data:
                    raise ValueError(f"Transaction {lineno} missing TRNAMT")
                if 'TRNTYPE' not in txn_data:
                    raise ValueError(f"Transaction {lineno} missing TRNTYPE")
                
                date = parse_ofx_datetime(txn_data['DTPOSTED'])
                amount = decimal.Decimal(txn_data['TRNAMT'])
                txn_type = txn_data['TRNTYPE']
                desc = txn_data.get('NAME', '')
                note = txn_data.get('MEMO')
                source_account = txn_data.get('ACCOUNT_ID')
                if not source_account:
                    raise ValueError(f"Transaction {lineno} missing ACCOUNT_ID")
                
                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=lineno,
                    transaction_id=txn_data['FITID'],
                    date=date,
                    post_date=date,
                    desc=desc,
                    amount=amount,
                    type=txn_type,
                    note=note,
                    currency=txn_data.get('CURRENCY', 'USD'),
                    source_account=source_account,
                    extra={},
                )
            except (ValueError, KeyError) as e:
                # Skip malformed transactions but continue processing
                continue

