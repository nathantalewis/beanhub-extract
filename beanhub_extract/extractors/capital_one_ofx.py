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


class CapitalOneOFXExtractor(ExtractorBase):
    EXTRACTOR_NAME = "capital_one_ofx"
    DEFAULT_IMPORT_ID = "capital_one_credit_card:{{ last_four_digits }}:{{ transaction_id }}"

    def detect(self) -> bool:
        """Detect if this is a Capital One OFX file."""
        try:
            self.input_file.seek(0)
            content = self.input_file.read(1000)  # Read first 1000 chars
            self.input_file.seek(0)
            
            # Check for OFX header and Capital One specific markers
            return (
                "<?OFX" in content and
                "<OFX>" in content and
                ("<ORG>C1</ORG>" in content or "<FID>1001</FID>" in content)
            )
        except Exception:
            return False

    def fingerprint(self) -> Fingerprint | None:
        """Generate fingerprint based on first and last transaction."""
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

            if not hasattr(stmt, 'ccacctfrom'):
                raise ValueError("OFX statement missing ccacctfrom - cannot extract account information")

            account_id = stmt.ccacctfrom.acctid
            if not account_id:
                raise ValueError("OFX statement ccacctfrom missing acctid - cannot identify account")

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
                    if hasattr(txn, 'dtuser') and txn.dtuser:
                        txn_data['DTUSER'] = txn.dtuser.strftime('%Y%m%d%H%M%S')
                    if hasattr(txn, 'memo') and txn.memo:
                        txn_data['MEMO'] = txn.memo
                    
                    # Extract last_four_digits from CCACCTTO if available
                    if hasattr(txn, 'ccacctto') and hasattr(txn.ccacctto, 'acctid'):
                        txn_data['CCACCTTO_ACCTID'] = txn.ccacctto.acctid
                    
                    txn_data['CURRENCY'] = currency
                    txn_data['ACCOUNT_ID'] = account_id
                    yield txn_data

            # Generate balance transaction if available
            if ledger_balance and ledger_date:
                yield {
                    'TRNTYPE': 'BALANCE',
                    'DTPOSTED': ledger_date,
                    'DTUSER': ledger_date,
                    'TRNAMT': ledger_balance,
                    'FITID': f"BALANCE_{ledger_date}",
                    'NAME': f"Balance as of {parse_ofx_datetime(ledger_date)}",
                    'CCACCTTO_ACCTID': account_id,  # Use main account ID for balance
                    'CURRENCY': currency,
                    'ACCOUNT_ID': account_id,
                }

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from Capital One OFX file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        for lineno, txn_data in enumerate(self._parse_transactions(), 1):
            try:
                # Validate required fields
                if 'DTPOSTED' not in txn_data:
                    raise ValueError(f"Transaction {lineno} missing DTPOSTED")
                if 'TRNAMT' not in txn_data:
                    raise ValueError(f"Transaction {lineno} missing TRNAMT")
                if 'ACCOUNT_ID' not in txn_data:
                    raise ValueError(f"Transaction {lineno} missing ACCOUNT_ID")

                # Parse transaction data
                transaction_date = parse_ofx_datetime(txn_data['DTPOSTED'])
                user_date = None
                if 'DTUSER' in txn_data:
                    user_date = parse_ofx_datetime(txn_data['DTUSER'])
                
                amount = decimal.Decimal(txn_data['TRNAMT'])
                
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
                last_four_digits = txn_data.get('CCACCTTO_ACCTID', '')
                
                # Use extracted currency or default to USD
                currency = txn_data.get('CURRENCY', 'USD')
                
                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=lineno,
                    transaction_id=txn_data.get('FITID'),
                    date=user_date,
                    post_date=transaction_date,  # OFX DTPOSTED is the post date
                    desc=desc,
                    amount=amount,
                    type=txn_type,
                    note=note,
                    currency=currency,
                    source_account=source_account,
                    last_four_digits=last_four_digits,
                    extra={k: v for k, v in txn_data.items() 
                          if k not in ['DTPOSTED', 'DTUSER', 'TRNAMT', 'NAME', 'MEMO', 'FITID', 'TRNTYPE', 
                                     'CCACCTTO_ACCTID', 'CURRENCY', 'ACCOUNT_ID']}
                )
            except (ValueError, KeyError) as e:
                # Skip malformed transactions but continue processing
                continue
