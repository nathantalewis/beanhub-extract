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


class EtradeOFXExtractor(ExtractorBase):
    EXTRACTOR_NAME = "etrade_ofx"
    DEFAULT_IMPORT_ID = "etrade:{{ source_account }}:{{ transaction_id }}"

    def detect(self) -> bool:
        """Detect if this is an E*TRADE OFX file."""
        try:
            self.input_file.seek(0)
            content = self.input_file.read(1000)  # Read first 1000 chars
            self.input_file.seek(0)
            
            # Check for OFX header and E*TRADE specific markers
            return (
                "OFXHEADER:" in content and
                "<OFX>" in content and
                ("<ORG>E*TRADE" in content or "<BROKERID>etrade.com" in content)
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
            hash_obj.update(str(first_txn.get('TOTAL', '')).encode('utf-8'))
            hash_obj.update(str(first_txn.get('MEMO', '')).encode('utf-8'))
            
            return Fingerprint(
                starting_date=parse_ofx_datetime(first_txn['DTTRADE']),
                first_row_hash=hash_obj.hexdigest(),
            )
        except Exception:
            return None

    def _parse_transactions(self) -> typing.Generator[dict, None, None]:
        """Parse OFX file and yield transaction dictionaries."""
        self.input_file.seek(0)
        content = self.input_file.read()
        self.input_file.seek(0)

        binary_file = BytesIO(content.encode('utf-8'))

        parser = OFXTree()
        parser.parse(binary_file)
        ofx = parser.convert()

        # Check if this is an investment statement
        if not hasattr(ofx, 'invstmtmsgsrsv1') or not ofx.invstmtmsgsrsv1:
            return

        for stmt_wrapper in ofx.invstmtmsgsrsv1:
            stmt = stmt_wrapper.invstmtrs
            currency = stmt.curdef if hasattr(stmt, 'curdef') else 'USD'

            if not hasattr(stmt, 'invacctfrom'):
                raise ValueError("OFX investment statement missing invacctfrom - cannot extract account information")

            account_id = stmt.invacctfrom.acctid
            if not account_id:
                raise ValueError("OFX investment statement invacctfrom missing acctid - cannot identify account")

            # Build securities lookup for CUSIP to ticker/name mapping
            securities_map = {}
            if hasattr(ofx, 'securities') and ofx.securities:
                for sec in ofx.securities:
                    if hasattr(sec, 'secinfo'):
                        secinfo = sec.secinfo
                        cusip = getattr(secinfo, 'uniqueid', None)
                        if cusip:
                            securities_map[cusip] = {
                                'ticker': getattr(secinfo, 'ticker', ''),
                                'name': getattr(secinfo, 'secname', ''),
                                'unitprice': getattr(secinfo, 'unitprice', None)
                            }

            # Process investment transactions
            if hasattr(stmt, 'invtranlist') and stmt.invtranlist:
                for txn in stmt.invtranlist:
                    txn_data = self._extract_transaction_data(txn, currency, account_id, securities_map)
                    if txn_data:
                        yield txn_data

            # Process balance information
            if hasattr(stmt, 'invbal'):
                balance_data = self._extract_balance_data(stmt.invbal, currency, account_id, stmt.dtasof)
                if balance_data:
                    yield balance_data

    def _extract_transaction_data(self, txn, currency: str, account_id: str, securities_map: dict) -> dict | None:
        """Extract transaction data from an investment transaction."""
        txn_type = type(txn).__name__
        
        # Get common transaction info
        invtran = getattr(txn, 'invtran', None)
        if invtran is None:
            return None
            
        fitid = getattr(invtran, 'fitid', '')
        dttrade = getattr(invtran, 'dttrade', None)
        memo = getattr(invtran, 'memo', '')
        
        # dttrade should be a datetime object, not None
        if dttrade is None:
            return None
            
        # Get security information if available
        secid = getattr(txn, 'secid', None)
        cusip = getattr(secid, 'uniqueid', '') if secid is not None else ''
        security_info = securities_map.get(cusip, {})
        
        # Extract transaction-specific data
        total = getattr(txn, 'total', None)
        units = getattr(txn, 'units', None)
        unitprice = getattr(txn, 'unitprice', None)
        fees = getattr(txn, 'fees', None)
        
        # For BUYSTOCK transactions, get data from invbuy
        if txn_type == 'BUYSTOCK' and hasattr(txn, 'invbuy'):
            invbuy = txn.invbuy
            if not total and hasattr(invbuy, 'total'):
                total = invbuy.total
            if not units and hasattr(invbuy, 'units'):
                units = invbuy.units
            if not unitprice and hasattr(invbuy, 'unitprice'):
                unitprice = invbuy.unitprice
            if not fees and hasattr(invbuy, 'fees'):
                fees = invbuy.fees
        
        return {
            'TRANSACTION_TYPE': txn_type,
            'FITID': fitid,
            'DTTRADE': dttrade.strftime('%Y%m%d%H%M%S'),
            'MEMO': memo,
            'TOTAL': str(total) if total is not None else '0',
            'UNITS': str(units) if units is not None else None,
            'UNITPRICE': str(unitprice) if unitprice is not None else None,
            'FEES': str(fees) if fees is not None else None,
            'CUSIP': cusip,
            'TICKER': security_info.get('ticker', ''),
            'SECURITY_NAME': security_info.get('name', ''),
            'CURRENCY': currency,
            'ACCOUNT_ID': account_id,
        }

    def _extract_balance_data(self, invbal, currency: str, account_id: str, dtasof: datetime.datetime) -> dict | None:
        """Extract balance information as a transaction."""
        if not hasattr(invbal, 'availcash'):
            return None
            
        return {
            'TRANSACTION_TYPE': 'BALANCE',
            'FITID': f"BALANCE_{dtasof.strftime('%Y%m%d%H%M%S')}",
            'DTTRADE': dtasof.strftime('%Y%m%d%H%M%S'),
            'MEMO': f"Available Cash Balance as of {dtasof.date()}",
            'TOTAL': str(invbal.availcash),
            'UNITS': None,
            'UNITPRICE': None,
            'FEES': None,
            'CUSIP': '',
            'TICKER': 'CASH',
            'SECURITY_NAME': 'Cash Balance',
            'CURRENCY': currency,
            'ACCOUNT_ID': account_id,
        }

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from E*TRADE OFX file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        for lineno, txn_data in enumerate(self._parse_transactions(), 1):
            # Validate required fields
            if 'DTTRADE' not in txn_data:
                raise ValueError(f"Transaction {lineno} missing DTTRADE")
            if 'TOTAL' not in txn_data:
                raise ValueError(f"Transaction {lineno} missing TOTAL")
            if 'TRANSACTION_TYPE' not in txn_data:
                raise ValueError(f"Transaction {lineno} missing TRANSACTION_TYPE")
            if 'ACCOUNT_ID' not in txn_data:
                raise ValueError(f"Transaction {lineno} missing ACCOUNT_ID")

            # Parse transaction data
            transaction_date = parse_ofx_datetime(txn_data['DTTRADE'])
            amount = decimal.Decimal(txn_data['TOTAL'])
            
            # Extract description and memo
            memo = txn_data.get('MEMO', '').strip()
            
            # Use memo as description, transaction type as note if no memo
            desc = memo if memo else txn_data['TRANSACTION_TYPE']
            note = None
            
            # Build extra data
            extra = {}
            for key in ['UNITS', 'UNITPRICE', 'FEES', 'CUSIP', 'TICKER', 'SECURITY_NAME']:
                value = txn_data.get(key)
                if value is not None and value != '':
                    extra[key.lower()] = value
            
            yield Transaction(
                extractor=self.EXTRACTOR_NAME,
                file=filename,
                lineno=lineno,
                transaction_id=txn_data.get('FITID'),
                date=transaction_date,
                post_date=transaction_date,
                desc=desc,
                amount=amount,
                type=txn_data['TRANSACTION_TYPE'],
                note=note,
                currency=txn_data.get('CURRENCY', 'USD'),
                source_account=txn_data['ACCOUNT_ID'],
                extra=extra,
            )
