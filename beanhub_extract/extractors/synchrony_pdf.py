import datetime
import decimal
import hashlib
import re
import typing

import pypdf

from ..data_types import Fingerprint, Transaction
from .base import ExtractorBase


def parse_date(date_str: str) -> datetime.date:
    """Parse date string like 'Aug 14, 2025' to date object"""
    try:
        return datetime.datetime.strptime(date_str.replace(',', ''), '%b %d %Y').date()
    except ValueError:
        return datetime.datetime.strptime(date_str.replace(',', ''), '%B %d %Y').date()


class SynchronyPdfExtractor(ExtractorBase):
    """
    Extractor for Synchrony Bank PDF statements.
    """
    
    EXTRACTOR_NAME = "synchrony_pdf"
    DEFAULT_IMPORT_ID = "{{ extractor }}:{{ source_account }}:{{ reversed_lineno }}"

    def _extract_pdf_text(self) -> str:
        """Extract text from PDF using pypdf"""
        try:
            # Handle both binary and text file objects
            if hasattr(self.input_file, 'mode') and 'b' not in self.input_file.mode:
                # File is opened in text mode, need to read from file path
                file_path = getattr(self.input_file, 'name', None)
                if file_path:
                    with open(file_path, 'rb') as f:
                        reader = pypdf.PdfReader(f)
                        all_text = ""
                        for page in reader.pages:
                            text = page.extract_text()
                            if text:
                                all_text += text + "\n"
                        return all_text
                else:
                    return ""
            else:
                # File is already in binary mode
                self.input_file.seek(0)
                reader = pypdf.PdfReader(self.input_file)
                all_text = ""
                
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        all_text += text + "\n"
                
                return all_text
            
        except Exception:
            return ""

    def detect(self) -> bool:
        """Detect if this is a Synchrony PDF statement"""
        try:
            # Check for PDF header first
            if hasattr(self.input_file, 'mode') and 'b' not in self.input_file.mode:
                # File is in text mode, need to check file path
                file_path = getattr(self.input_file, 'name', None)
                if file_path:
                    with open(file_path, 'rb') as f:
                        header = f.read(10)
                        if not header.startswith(b'%PDF-'):
                            return False
                else:
                    return False
            else:
                # File is in binary mode
                self.input_file.seek(0)
                header = self.input_file.read(10)
                self.input_file.seek(0)
                if not header.startswith(b'%PDF-'):
                    return False
                
            text = self._extract_pdf_text()
            if not text:
                return False
                
            # Check for Synchrony-specific indicators
            text_upper = text.upper()
            return (
                "SYNCHRONYBANK.COM" in text_upper or 
                "SYNCHRONY BANK" in text_upper or
                "SYNCHRONY FINANCIAL" in text_upper
            )
            
        except Exception:
            return False

    def fingerprint(self) -> Fingerprint | None:
        """Generate fingerprint for the PDF"""
        try:
            text = self._extract_pdf_text()
            if not text:
                return None
                
            # Create hash from the entire text content
            hash_obj = hashlib.sha256()
            hash_obj.update(text.encode("utf-8"))
            
            # Extract the earliest date from transactions
            transactions = list(self._extract_transactions())
            if not transactions:
                starting_date = datetime.date(1970, 1, 1)
            else:
                dates = [txn.date for txn in transactions if txn.date]
                starting_date = min(dates) if dates else datetime.date(1970, 1, 1)
            
            return Fingerprint(
                starting_date=starting_date,
                first_row_hash=hash_obj.hexdigest()[:16]  # Use first 16 chars
            )
            
        except Exception:
            return None

    def _extract_transactions(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from PDF text"""
        text = self._extract_pdf_text()
        if not text:
            return
            
        filename = getattr(self.input_file, 'name', None)
        
        # Extract account number
        account_match = re.search(r'XXXXXXXX(\d+)', text)
        account_number = account_match.group(1) if account_match else None
        
        # Extract current balance and date
        balance_pattern = r'Current\s+Balance\s+\$(\d{1,3}(?:,\d{3})*\.\d{2})'
        balance_match = re.search(balance_pattern, text, re.IGNORECASE)
        current_balance = balance_match.group(1).replace(',', '') if balance_match else None
        
        # Extract transactions: Date + Description + Amount
        transaction_pattern = r'((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4})\s+(.+?)\s+\$(\d{1,3}(?:,\d{3})*\.\d{2})'
        transaction_matches = re.findall(transaction_pattern, text, re.IGNORECASE)
        
        # Find the most recent transaction date for balance date
        balance_date = None
        if transaction_matches and current_balance:
            try:
                # Use one day after the most recent transaction date as balance date
                latest_date_str = transaction_matches[0][0]  # Assuming first is most recent
                latest_date = parse_date(latest_date_str.strip())
                balance_date = latest_date + datetime.timedelta(days=1)
            except (ValueError, IndexError):
                pass
        
        lineno = 1
        for date_str, description, amount_str in transaction_matches:
            try:
                date_obj = parse_date(date_str.strip())
                amount = decimal.Decimal(amount_str.replace(',', ''))
                desc = description.strip()
                
                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=lineno,
                    reversed_lineno=lineno - len(transaction_matches) - (1 if current_balance else 0),
                    date=date_obj,
                    desc=desc,
                    amount=amount,
                    currency="USD",
                    source_account=account_number,
                )
                lineno += 1
                
            except (ValueError, decimal.InvalidOperation):
                continue
        
        # Yield balance transaction if available
        if current_balance and balance_date:
            yield Transaction(
                extractor=self.EXTRACTOR_NAME,
                file=filename,
                lineno=lineno,
                reversed_lineno=0,  # Balance is always the most recent
                transaction_id=f"BALANCE_{balance_date.isoformat()}",
                date=balance_date,
                desc=f"Balance as of {balance_date.strftime('%Y-%m-%d')}",
                amount=decimal.Decimal(current_balance),
                currency="USD",
                type="BALANCE",
                source_account=account_number,
            )

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Main extraction method"""
        yield from self._extract_transactions()
