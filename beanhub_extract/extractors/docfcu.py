import csv
import datetime
import decimal
import hashlib
import os
import typing

from ..data_types import Fingerprint
from ..data_types import Transaction
from .base import ExtractorBase


def parse_date(date_str: str) -> datetime.date:
    """Parse MM/DD/YYYY date format."""
    return datetime.datetime.strptime(date_str, "%m/%d/%Y").date()


def parse_currency_amount(amount_str: str) -> decimal.Decimal:
    """Parse currency amount, handling empty strings and negative values."""
    if not amount_str or amount_str.strip() == "":
        return decimal.Decimal("0.00")
    
    # Remove quotes and commas
    cleaned = amount_str.replace('"', '').replace(',', '').strip()
    if not cleaned:
        return decimal.Decimal("0.00")
    
    return decimal.Decimal(cleaned)


class DocfcuExtractor(ExtractorBase):
    EXTRACTOR_NAME = "docfcu"
    DEFAULT_IMPORT_ID = "docfcu:{{ source_account }}:{{ transaction_id }}"
    
    EXPECTED_HEADERS = [
        "Transaction Number",
        "Date",
        "Description",
        "Memo",
        "Amount Debit",
        "Amount Credit",
        "Balance",
        "Check Number",
        "Fees ",
        "Principal ",
        "Interest"
    ]

    def detect(self) -> bool:
        """Detect if this is a DOCFCU CSV file."""
        try:
            self.input_file.seek(0)
            lines = self.input_file.readlines()
            
            # Check if we have enough lines
            if len(lines) < 4:
                return False
            
            # Check for DOCFCU-specific header pattern
            first_line = lines[0].strip()
            if not first_line.startswith('"Account Name :'):
                return False
            
            # Check the column headers on line 4 (index 3)
            header_line = lines[3].strip()
            reader = csv.reader([header_line])
            headers = next(reader)
            
            return headers == self.EXPECTED_HEADERS
        except Exception:
            return False

    def fingerprint(self) -> Fingerprint | None:
        """Generate fingerprint based on the first transaction."""
        try:
            self.input_file.seek(0)
            lines = self.input_file.readlines()
            
            if len(lines) < 5:  # Need at least header + 1 transaction
                return None
            
            # Skip header lines and get first transaction
            transaction_lines = lines[4:]  # Skip first 4 lines (headers)
            if not transaction_lines:
                return None
            
            reader = csv.DictReader(transaction_lines, fieldnames=self.EXPECTED_HEADERS)
            try:
                row = next(reader)
            except StopIteration:
                return None
            
            # Create hash from first transaction
            hash_obj = hashlib.sha256()
            for field in self.EXPECTED_HEADERS:
                hash_obj.update(row[field].encode("utf8"))
            
            return Fingerprint(
                starting_date=parse_date(row["Date"]),
                first_row_hash=hash_obj.hexdigest(),
            )
        except Exception:
            return None

    def _extract_account_info(self) -> tuple[str, str]:
        """Extract account name and number from header lines."""
        self.input_file.seek(0)
        lines = self.input_file.readlines()
        
        account_name = ""
        account_number = ""
        
        if len(lines) >= 2:
            # Parse account name from first line
            first_line = lines[0].strip().replace('"', '')
            if first_line.startswith("Account Name :"):
                account_name = first_line.split(":", 1)[1].strip()
            
            # Parse account number from second line
            second_line = lines[1].strip().replace('"', '')
            if second_line.startswith("Account Number :"):
                account_number = second_line.split(":", 1)[1].strip()
        
        return account_name, account_number

    def _find_most_recent_transaction(self) -> dict | None:
        """Find the transaction with the most recent date for balance purposes.
        Since DOCFCU CSV is ordered with most recent transactions first,
        we need to find the first valid non-comment transaction.
        """
        self.input_file.seek(0)
        lines = self.input_file.readlines()
        
        if len(lines) < 5:  # Need headers + at least 1 transaction
            return None
        
        transaction_lines = lines[4:]  # Skip first 4 lines
        reader = csv.DictReader(transaction_lines, fieldnames=self.EXPECTED_HEADERS)
        
        for row in reader:
            try:
                # Skip empty lines
                if not any(row.values()):
                    continue
                
                # Skip comment lines
                description = row["Description"].strip()
                if description == "COMMENT":
                    continue
                
                # Validate that this row has a valid date and balance
                parse_date(row["Date"])
                balance_str = row["Balance"].strip()
                if balance_str:  # Only return transactions with balance info
                    return row
            except (ValueError, IndexError):
                continue
        
        return None

    def _create_balance_transaction(self, balance_row: dict, filename: str | None) -> Transaction:
        """Create a balance transaction from the most recent transaction row."""
        transaction_date = parse_date(balance_row["Date"])
        # Balance assertion should be dated the day after the most recent transaction
        balance_date = transaction_date + datetime.timedelta(days=1)
        balance_amount = parse_currency_amount(balance_row["Balance"])
        
        # Get account info
        account_name, account_number = self._extract_account_info()
        source_account = account_number if account_number else account_name
        
        return Transaction(
            extractor=self.EXTRACTOR_NAME,
            file=filename,
            lineno=0,  # Balance transaction doesn't correspond to a specific line
            transaction_id=f"BALANCE_{transaction_date.strftime('%Y%m%d')}_{source_account}",
            date=balance_date,
            post_date=balance_date,
            desc=f"Balance as of {transaction_date.strftime('%Y-%m-%d')}",
            amount=balance_amount,
            currency="USD",  # DOCFCU is US-based credit union
            type="BALANCE",
            source_account=source_account,
        )

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from DOCFCU CSV file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        # Extract account information
        account_name, account_number = self._extract_account_info()
        source_account = account_number if account_number else account_name

        # Skip header lines and process transactions
        self.input_file.seek(0)
        lines = self.input_file.readlines()
        
        if len(lines) < 5:  # Need headers + at least 1 transaction
            return
        
        transaction_lines = lines[4:]  # Skip first 4 lines
        reader = csv.DictReader(transaction_lines, fieldnames=self.EXPECTED_HEADERS)
        
        for i, row in enumerate(reader):
            try:
                # Skip empty lines
                if not any(row.values()):
                    continue
                
                # Parse basic transaction data
                transaction_date = parse_date(row["Date"])
                description = row["Description"].strip()
                memo = row["Memo"].strip()
                transaction_number = row["Transaction Number"].strip()
                
                # Skip comment lines - these are informational only
                if description == "COMMENT":
                    continue
                
                # Parse amounts - DOCFCU uses separate debit/credit columns
                debit_amount = parse_currency_amount(row["Amount Debit"])
                credit_amount = parse_currency_amount(row["Amount Credit"])
                
                # Determine net amount (credits are positive, debits are negative)
                if credit_amount != decimal.Decimal("0.00"):
                    amount = credit_amount
                elif debit_amount != decimal.Decimal("0.00"):
                    amount = -debit_amount
                else:
                    continue  # Skip transactions with no amount
                
                # Parse optional fields
                balance_str = row["Balance"].strip()
                check_number = row["Check Number"].strip() if row["Check Number"].strip() else None
                fees = parse_currency_amount(row["Fees "])
                principal = parse_currency_amount(row["Principal "])
                interest = parse_currency_amount(row["Interest"])
                
                # Build extra data
                extra = {}
                if balance_str:
                    extra["balance"] = balance_str
                if check_number:
                    extra["check_number"] = check_number
                # Always include fees and interest, even if zero
                extra["fees"] = str(fees)
                extra["principal"] = str(principal)
                extra["interest"] = str(interest)
                if memo:
                    extra["memo"] = memo
                
                # Combine description and memo for full description
                full_desc = description
                if memo and memo != description:
                    full_desc = f"{description} - {memo}"

                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=i + 5,  # Account for header lines
                    transaction_id=transaction_number,
                    date=transaction_date,
                    post_date=transaction_date,
                    desc=full_desc,
                    amount=amount,
                    currency="USD",  # DOCFCU is US-based credit union
                    source_account=source_account,
                    extra=extra if extra else None,
                )
            except (ValueError, KeyError, decimal.InvalidOperation, IndexError) as e:
                # Skip malformed transactions but continue processing
                continue

        # Add balance transaction using the most recent transaction
        most_recent_row = self._find_most_recent_transaction()
        if most_recent_row:
            yield self._create_balance_transaction(most_recent_row, filename)
