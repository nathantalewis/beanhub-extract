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


class AllyBankOFXExtractor(ExtractorBase):
    EXTRACTOR_NAME = "ally_bank_ofx"
    DEFAULT_IMPORT_ID = "ally_bank:{{ source_account }}:{{ transaction_id }}"

    def detect(self) -> bool:
        """Detect if this is an Ally Bank OFX file."""
        try:
            self.input_file.seek(0)
            content = self.input_file.read(1000)  # Read first 1000 chars
            self.input_file.seek(0)
            
            # Check for OFX header and Ally Bank specific markers
            return (
                "OFXHEADER:" in content and
                "<OFX>" in content and
                ("<ORG>Ally</ORG>" in content or "<FID>6157</FID>" in content)
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

        # Fix case sensitivity issues in OFX SEVERITY tags for ofxtools compatibility
        content = content.replace('<SEVERITY>Info</SEVERITY>', '<SEVERITY>INFO</SEVERITY>')
        content = content.replace('<SEVERITY>Warn</SEVERITY>', '<SEVERITY>WARN</SEVERITY>')
        content = content.replace('<SEVERITY>Error</SEVERITY>', '<SEVERITY>ERROR</SEVERITY>')

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

            ledger_balance = None
            ledger_date = None
            if hasattr(stmt, 'ledgerbal') and hasattr(stmt.ledgerbal, 'balamt'):
                ledger_balance = str(stmt.ledgerbal.balamt)
                ledger_date = stmt.ledgerbal.dtasof.strftime('%Y%m%d%H%M%S')

            if hasattr(stmt, 'banktranlist') and stmt.banktranlist:
                transactions = list(stmt.banktranlist)
                for txn in transactions:
                    txn_data = {
                        'TRNTYPE': txn.trntype,
                        'DTPOSTED': txn.dtposted.strftime('%Y%m%d%H%M%S'),
                        'TRNAMT': str(txn.trnamt),
                        'FITID': txn.fitid,
                        'NAME': txn.name,
                    }
                    if hasattr(txn, 'memo') and txn.memo:
                        txn_data['MEMO'] = txn.memo
                    txn_data['CURRENCY'] = currency
                    txn_data['ACCOUNT_ID'] = account_id
                    yield txn_data

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
        """Extract transactions from Ally Bank OFX file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        for lineno, txn_data in enumerate(self._parse_transactions(), 1):
            # Validate required fields
            if 'DTPOSTED' not in txn_data:
                raise ValueError(f"Transaction {lineno} missing DTPOSTED")
            if 'TRNAMT' not in txn_data:
                raise ValueError(f"Transaction {lineno} missing TRNAMT")
            if 'TRNTYPE' not in txn_data:
                raise ValueError(f"Transaction {lineno} missing TRNTYPE")
            if 'ACCOUNT_ID' not in txn_data:
                raise ValueError(f"Transaction {lineno} missing ACCOUNT_ID")

            # Parse transaction data
            transaction_date = parse_ofx_datetime(txn_data['DTPOSTED'])
            amount = decimal.Decimal(txn_data['TRNAMT'])
            
            # Extract description and memo
            name = txn_data.get('NAME', '').strip()
            memo = txn_data.get('MEMO', '').strip()
            
            # Use name as primary description, memo as note if different
            desc = name
            note = memo if memo and memo != name else None
            
            yield Transaction(
                extractor=self.EXTRACTOR_NAME,
                file=filename,
                lineno=lineno,
                transaction_id=txn_data.get('FITID'),
                date=transaction_date,
                post_date=transaction_date,
                desc=desc,
                amount=amount,
                type=txn_data['TRNTYPE'],
                note=note,
                currency=txn_data.get('CURRENCY', 'USD'),
                source_account=txn_data['ACCOUNT_ID'],
                last_four_digits=txn_data['ACCOUNT_ID'],
                extra={},
            )
