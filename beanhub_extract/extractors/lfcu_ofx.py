import datetime
import decimal
import hashlib
import typing
from io import BytesIO

from ofxtools.Parser import OFXTree

from ..data_types import Fingerprint
from ..data_types import Transaction
from .base import ExtractorBase


def parse_ofx_datetime(dt_str: str) -> datetime.date:
    """Parse OFX datetime format (YYYYMMDDHHMMSS.sss) to date."""
    # OFX datetime format is YYYYMMDDHHMMSS.sss, we only need the date part
    date_part = dt_str[:8]  # YYYYMMDD
    return datetime.datetime.strptime(date_part, "%Y%m%d").date()


class LfcuOFXExtractor(ExtractorBase):
    EXTRACTOR_NAME = "lfcu_ofx"
    DEFAULT_IMPORT_ID = "lfcu:{{ source_account }}:{{ transaction_id }}"

    def detect(self) -> bool:
        """Detect if this is an LFCU OFX file."""
        try:
            self.input_file.seek(0)
            content = self.input_file.read(1000)  # Read first 1000 chars
            self.input_file.seek(0)
            
            # Check for OFX header and LFCU specific markers
            return (
                "OFXHEADER:" in content and
                "<OFX>" in content and
                ("<ORG>Lafayette Federal Credit Union</ORG>" in content or "<FID>16710</FID>" in content)
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
        content = self.input_file.read()
        self.input_file.seek(0)

        from io import BytesIO
        binary_file = BytesIO(content.encode('utf-8'))

        parser = OFXTree()
        parser.parse(binary_file)
        ofx = parser.convert()

        if not hasattr(ofx, 'statements') or not ofx.statements:
            return

        for stmt in ofx.statements:
            currency = stmt.curdef if hasattr(stmt, 'curdef') else 'USD'

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

            if hasattr(stmt, 'banktranlist') and stmt.banktranlist:
                transactions = list(stmt.banktranlist)
                for lineno, txn in enumerate(transactions, 1):
                    # Validate required fields
                    if not hasattr(txn, 'dtposted'):
                        raise ValueError(f"Transaction {lineno} missing DTPOSTED")
                    if not hasattr(txn, 'trnamt'):
                        raise ValueError(f"Transaction {lineno} missing TRNAMT")
                    if not hasattr(txn, 'trntype'):
                        raise ValueError(f"Transaction {lineno} missing TRNTYPE")

                    txn_data = {
                        'TRNTYPE': txn.trntype,
                        'DTPOSTED': txn.dtposted.strftime('%Y%m%d%H%M%S'),
                        'TRNAMT': str(txn.trnamt),
                        'FITID': txn.fitid if hasattr(txn, 'fitid') else f"TXN_{lineno}",
                        'NAME': txn.name if hasattr(txn, 'name') else '',
                    }
                    
                    # Optional fields
                    if hasattr(txn, 'memo') and txn.memo:
                        txn_data['MEMO'] = txn.memo
                    
                    txn_data['CURRENCY'] = currency
                    txn_data['ACCOUNT_ID'] = account_id
                    yield txn_data

            # Generate balance transaction if available
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

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from LFCU OFX file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        transactions = list(self._parse_transactions())
        
        for i, txn_data in enumerate(transactions):
            try:
                # Parse transaction data - LFCU only uses DTPOSTED
                transaction_date = parse_ofx_datetime(txn_data.get('DTPOSTED', ''))
                user_date = transaction_date  # LFCU doesn't have DTUSER, use DTPOSTED for both
                
                amount = decimal.Decimal(txn_data.get('TRNAMT', '0'))
                
                # Extract description and memo
                name = txn_data.get('NAME', '').strip()
                memo = txn_data.get('MEMO', '').strip()
                
                # Use name as primary description, memo as note if different
                desc = name
                note = memo if memo and memo != name else None
                
                # Determine transaction type
                txn_type = txn_data.get('TRNTYPE', '')
                
                # Extract account information
                source_account = txn_data.get('ACCOUNT_ID')
                
                # Use extracted currency or default to USD
                currency = txn_data.get('CURRENCY', 'USD')
                
                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=i + 1,
                    transaction_id=txn_data.get('FITID'),
                    date=user_date,
                    post_date=transaction_date,  # OFX DTPOSTED is the post date
                    desc=desc,
                    amount=amount,
                    type=txn_type,
                    note=note,
                    currency=currency,
                    source_account=source_account,
                    extra={k: v for k, v in txn_data.items() 
                          if k not in ['DTPOSTED', 'TRNAMT', 'NAME', 'MEMO', 'FITID', 'TRNTYPE', 
                                     'CURRENCY', 'ACCOUNT_ID']}
                )
            except (ValueError, KeyError) as e:
                # Skip malformed transactions but continue processing
                continue
