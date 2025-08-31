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


class AllyBankOFXExtractor(ExtractorBase):
    EXTRACTOR_NAME = "ally_bank_ofx"
    DEFAULT_IMPORT_ID = "ally_bank:{{ account_id }}:{{ transaction_id }}"

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

    def _extract_statement_metadata(self, stmtrs) -> tuple[str | None, str | None, str | None, str | None, str | None]:
        """Extract currency, main account, ledger balance, ledger date, and account ID from statement."""
        currency = None
        curdef = stmtrs.find('.//CURDEF')
        if curdef is not None:
            currency = curdef.text
        
        main_account = None
        account_id = None
        bankacctfrom = stmtrs.find('.//BANKACCTFROM/ACCTID')
        if bankacctfrom is not None:
            main_account = bankacctfrom.text
            account_id = bankacctfrom.text
        
        ledger_balance = None
        ledger_date = None
        ledgerbal = stmtrs.find('.//LEDGERBAL')
        if ledgerbal is not None:
            balamt = ledgerbal.find('BALAMT')
            dtasof = ledgerbal.find('DTASOF')
            if balamt is not None:
                ledger_balance = balamt.text
            if dtasof is not None:
                ledger_date = dtasof.text
        
        return currency, main_account, ledger_balance, ledger_date, account_id

    def _create_balance_transaction(self, ledger_balance: str, ledger_date: str, 
                                  currency: str | None, main_account: str | None, 
                                  account_id: str | None = None) -> dict:
        """Create a balance transaction dictionary."""
        balance_txn = {
            'TRNTYPE': 'BALANCE',
            'DTPOSTED': ledger_date,
            'TRNAMT': ledger_balance,
            'FITID': f'BALANCE_{ledger_date}',
            'NAME': f'Balance as of {parse_ofx_datetime(ledger_date).strftime("%Y-%m-%d")}',
            '_CURRENCY': currency,
            '_MAIN_ACCOUNT': main_account
        }
        
        # Add account_id if available
        if account_id is not None:
            balance_txn['_ACCOUNT_ID'] = account_id
            
        return balance_txn

    def _add_metadata_to_transaction(self, txn_data: dict, currency: str | None, 
                                   main_account: str | None, ledger_balance: str | None, 
                                   ledger_date: str | None, account_id: str | None = None) -> None:
        """Add extracted metadata to transaction data."""
        if currency:
            txn_data['_CURRENCY'] = currency
        if main_account:
            txn_data['_MAIN_ACCOUNT'] = main_account
        if ledger_balance:
            txn_data['_LEDGER_BALANCE'] = ledger_balance
        if ledger_date:
            txn_data['_LEDGER_DATE'] = ledger_date
        if account_id:
            txn_data['_ACCOUNT_ID'] = account_id

    def _parse_transaction_from_element(self, stmt_trn) -> dict:
        """Parse transaction data from a STMTTRN element."""
        txn_data = {}
        for child in stmt_trn:
            txn_data[child.tag] = child.text
        return txn_data

    def _parse_transactions(self) -> typing.Generator[dict, None, None]:
        """Parse OFX file and yield transaction dictionaries."""
        self.input_file.seek(0)
        
        try:
            parser = OFXTree()
            parser.parse(self.input_file)
            ofx = parser.convert()
            
            # Navigate to bank statement transactions
            # Structure: OFX -> BANKMSGSRSV1 -> STMTTRNRS -> STMTRS -> BANKTRANLIST -> STMTTRN
            bank_msgs = ofx.find('.//BANKMSGSRSV1')
            if bank_msgs is None:
                return
                
            stmt_trnrs = bank_msgs.find('.//STMTTRNRS')
            if stmt_trnrs is None:
                return
                
            stmtrs = stmt_trnrs.find('.//STMTRS')
            if stmtrs is None:
                return
            
            # Extract statement metadata
            currency, main_account, ledger_balance, ledger_date, account_id = self._extract_statement_metadata(stmtrs)
                
            bank_tran_list = stmtrs.find('.//BANKTRANLIST')
            if bank_tran_list is None:
                return
            
            # Extract all STMTTRN elements
            for stmt_trn in bank_tran_list.findall('.//STMTTRN'):
                txn_data = self._parse_transaction_from_element(stmt_trn)
                self._add_metadata_to_transaction(txn_data, currency, main_account, ledger_balance, ledger_date, account_id)
                yield txn_data
            
            # Yield balance transaction if available
            if ledger_balance and ledger_date:
                yield self._create_balance_transaction(ledger_balance, ledger_date, currency, main_account, account_id)
                
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
                currency, main_account, ledger_balance, ledger_date, account_id = self._extract_statement_metadata(root)
                
                # Find all STMTTRN elements
                for stmt_trn in root.findall('.//STMTTRN'):
                    txn_data = self._parse_transaction_from_element(stmt_trn)
                    self._add_metadata_to_transaction(txn_data, currency, main_account, ledger_balance, ledger_date, account_id)
                    
                    if txn_data:  # Only yield if we have data
                        yield txn_data
                
                # Yield balance transaction if available
                if ledger_balance and ledger_date:
                    yield self._create_balance_transaction(ledger_balance, ledger_date, currency, main_account, account_id)
            except Exception:
                return

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from Ally Bank OFX file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        transactions = list(self._parse_transactions())
        
        for i, txn_data in enumerate(transactions):
            try:
                # Parse transaction data - Ally Bank only uses DTPOSTED
                transaction_date = parse_ofx_datetime(txn_data.get('DTPOSTED', ''))
                user_date = transaction_date  # Ally Bank doesn't have DTUSER, use DTPOSTED for both
                
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
                account_id = txn_data.get('_ACCOUNT_ID')
                
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
                    last_four_digits=account_id,
                    extra={k: v for k, v in txn_data.items() 
                          if k not in ['DTPOSTED', 'TRNAMT', 'NAME', 'MEMO', 'FITID', 'TRNTYPE', 
                                     '_CURRENCY', '_MAIN_ACCOUNT', '_LEDGER_BALANCE', 
                                     '_LEDGER_DATE', '_ACCOUNT_ID']}
                )
            except (ValueError, KeyError) as e:
                # Skip malformed transactions but continue processing
                continue
