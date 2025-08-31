import datetime
import decimal
import hashlib
import os
import typing
from xml.etree import ElementTree

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

    def _extract_statement_metadata(self, cc_stmtrs) -> tuple[str | None, str | None, str | None, str | None, str | None]:
        """Extract currency, main account, ledger balance, ledger date, and last four digits from statement."""
        currency = None
        curdef = cc_stmtrs.find('.//CURDEF')
        if curdef is not None:
            currency = curdef.text
        
        main_account = None
        ccacctfrom = cc_stmtrs.find('.//CCACCTFROM/ACCTID')
        if ccacctfrom is not None:
            main_account = ccacctfrom.text
        
        # For balance transactions, the last four digits is the same as main_account
        last_four_digits = main_account
        
        ledger_balance = None
        ledger_date = None
        ledgerbal = cc_stmtrs.find('.//LEDGERBAL')
        if ledgerbal is not None:
            balamt = ledgerbal.find('BALAMT')
            dtasof = ledgerbal.find('DTASOF')
            if balamt is not None:
                ledger_balance = balamt.text
            if dtasof is not None:
                ledger_date = dtasof.text
        
        return currency, main_account, ledger_balance, ledger_date, last_four_digits

    def _create_balance_transaction(self, ledger_balance: str, ledger_date: str, 
                                  currency: str | None, main_account: str | None, 
                                  last_four_digits: str | None = None) -> dict:
        """Create a balance transaction dictionary."""
        balance_txn = {
            'TRNTYPE': 'BALANCE',
            'DTPOSTED': ledger_date,
            'DTUSER': ledger_date,
            'TRNAMT': ledger_balance,
            'FITID': f'BALANCE_{ledger_date}',
            'NAME': f'Balance as of {parse_ofx_datetime(ledger_date).strftime("%Y-%m-%d")}',
            '_CURRENCY': currency,
            '_MAIN_ACCOUNT': main_account
        }
        
        # Add last_four_digits if available
        if last_four_digits is not None:
            balance_txn['CCACCTTO_ACCTID'] = last_four_digits
            
        return balance_txn

    def _add_metadata_to_transaction(self, txn_data: dict, currency: str | None, 
                                   main_account: str | None, ledger_balance: str | None, 
                                   ledger_date: str | None) -> None:
        """Add extracted metadata to transaction data."""
        if currency:
            txn_data['_CURRENCY'] = currency
        if main_account:
            txn_data['_MAIN_ACCOUNT'] = main_account
        if ledger_balance:
            txn_data['_LEDGER_BALANCE'] = ledger_balance
        if ledger_date:
            txn_data['_LEDGER_DATE'] = ledger_date

    def _parse_transaction_from_element(self, stmt_trn) -> dict:
        """Parse transaction data from a STMTTRN element."""
        txn_data = {}
        for child in stmt_trn:
            if child.tag == 'CCACCTTO':
                # Extract account ID from nested CCACCTTO
                acctid = child.find('ACCTID')
                if acctid is not None:
                    txn_data['CCACCTTO_ACCTID'] = acctid.text
            else:
                txn_data[child.tag] = child.text
        return txn_data

    def _parse_transactions(self) -> typing.Generator[dict, None, None]:
        """Parse OFX file and yield transaction dictionaries."""
        self.input_file.seek(0)
        
        try:
            parser = OFXTree()
            parser.parse(self.input_file)
            ofx = parser.convert()
            
            # Navigate to credit card statement transactions
            # Structure: OFX -> CREDITCARDMSGSRSV1 -> CCSTMTTRNRS -> CCSTMTRS -> BANKTRANLIST -> STMTTRN
            cc_msgs = ofx.find('.//CREDITCARDMSGSRSV1')
            if cc_msgs is None:
                return
                
            cc_stmt_trnrs = cc_msgs.find('.//CCSTMTTRNRS')
            if cc_stmt_trnrs is None:
                return
                
            cc_stmtrs = cc_stmt_trnrs.find('.//CCSTMTRS')
            if cc_stmtrs is None:
                return
            
            # Extract statement metadata
            currency, main_account, ledger_balance, ledger_date, last_four_digits = self._extract_statement_metadata(cc_stmtrs)
                
            bank_tran_list = cc_stmtrs.find('.//BANKTRANLIST')
            if bank_tran_list is None:
                return
            
            # Extract all STMTTRN elements
            for stmt_trn in bank_tran_list.findall('.//STMTTRN'):
                txn_data = self._parse_transaction_from_element(stmt_trn)
                self._add_metadata_to_transaction(txn_data, currency, main_account, ledger_balance, ledger_date)
                yield txn_data
            
            # Yield balance transaction if available
            if ledger_balance and ledger_date:
                yield self._create_balance_transaction(ledger_balance, ledger_date, currency, main_account, last_four_digits)
                
        except Exception as e:
            # If ofxtools parsing fails, try basic XML parsing
            self.input_file.seek(0)
            try:
                content = self.input_file.read()
                # Remove OFX header lines that aren't valid XML
                xml_start = content.find('<OFX>')
                if xml_start > 0:
                    content = content[xml_start:]
                
                root = ElementTree.fromstring(content)
                
                # Extract statement metadata
                currency, main_account, ledger_balance, ledger_date, last_four_digits = self._extract_statement_metadata(root)
                
                # Find all STMTTRN elements
                for stmt_trn in root.findall('.//STMTTRN'):
                    txn_data = self._parse_transaction_from_element(stmt_trn)
                    self._add_metadata_to_transaction(txn_data, currency, main_account, ledger_balance, ledger_date)
                    
                    if txn_data:  # Only yield if we have data
                        yield txn_data
                
                # Yield balance transaction if available
                if ledger_balance and ledger_date:
                    yield self._create_balance_transaction(ledger_balance, ledger_date, currency, main_account, last_four_digits)
            except Exception:
                return

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from Capital One OFX file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        transactions = list(self._parse_transactions())
        
        for i, txn_data in enumerate(transactions):
            try:
                # Parse transaction data
                transaction_date = parse_ofx_datetime(txn_data.get('DTPOSTED', ''))
                user_date = None
                if 'DTUSER' in txn_data:
                    user_date = parse_ofx_datetime(txn_data['DTUSER'])
                
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
                source_account = txn_data.get('_MAIN_ACCOUNT')
                last_four_digits = txn_data.get('CCACCTTO_ACCTID')
                
                # Use extracted currency or default to USD
                currency = txn_data.get('_CURRENCY', 'USD')
                
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
                    last_four_digits=last_four_digits,
                    extra={k: v for k, v in txn_data.items() 
                          if k not in ['DTPOSTED', 'DTUSER', 'TRNAMT', 'NAME', 'MEMO', 'FITID', 'TRNTYPE', 
                                     'CCACCTTO_ACCTID', '_CURRENCY', '_MAIN_ACCOUNT', '_LEDGER_BALANCE', 
                                     '_LEDGER_DATE']}
                )
            except (ValueError, KeyError) as e:
                # Skip malformed transactions but continue processing
                continue
