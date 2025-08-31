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
    parts = date_str.split("/")
    return datetime.date(int(parts[2]), int(parts[0]), int(parts[1]))


class WSECUExtractor(ExtractorBase):
    EXTRACTOR_NAME = "wsecu"
    DEFAULT_IMPORT_ID = "wsecu:{{ source_account }}:{{ reversed_lineno }}"
    ALL_FIELDS = [
        "Account number",
        "Date", 
        "Description",
        "Category",
        "Note",
        "Amount",
        "Balance",
    ]

    def detect(self) -> bool:
        """Detect if this is a WSECU CSV file."""
        try:
            self.input_file.seek(0)
            reader = csv.DictReader(self.input_file)
            return reader.fieldnames == self.ALL_FIELDS
        except Exception:
            return False

    def fingerprint(self) -> Fingerprint | None:
        """Generate fingerprint based on the most recent transaction."""
        try:
            # Use the helper method to find the most recent transaction
            row = self._find_most_recent_transaction()
            if row is None:
                return None
            
            # Get field names for hashing
            self.input_file.seek(0)
            reader = csv.DictReader(self.input_file)
            
            # Create hash from most recent transaction details
            hash_obj = hashlib.sha256()
            for field in reader.fieldnames:
                hash_obj.update(row[field].encode("utf8"))
            
            return Fingerprint(
                starting_date=parse_date(row["Date"]),
                first_row_hash=hash_obj.hexdigest(),
            )
        except Exception:
            return None

    def _find_most_recent_transaction(self) -> dict | None:
        """Find the transaction with the most recent date for balance purposes.
        Since WSECU CSV is ordered with most recent transactions first, 
        we just need the first valid row.
        """
        self.input_file.seek(0)
        reader = csv.DictReader(self.input_file)
        
        for row in reader:
            try:
                # Validate that this row has a valid date
                parse_date(row["Date"])
                return row
            except (ValueError, IndexError):
                continue
        
        return None

    def _create_balance_transaction(self, balance_row: dict, filename: str | None) -> Transaction:
        """Create a balance transaction from the most recent transaction row."""
        transaction_date = parse_date(balance_row["Date"])
        # Balance assertion should be dated the day after the most recent transaction
        balance_date = transaction_date + datetime.timedelta(days=1)
        balance_amount = decimal.Decimal(balance_row["Balance"])
        account_number = balance_row["Account number"]
        
        return Transaction(
            extractor=self.EXTRACTOR_NAME,
            file=filename,
            lineno=0,  # Balance transaction doesn't correspond to a specific line
            transaction_id=f"BALANCE_{transaction_date.strftime('%Y%m%d')}_{account_number}",
            date=balance_date,
            post_date=balance_date,
            desc=f"Balance as of {transaction_date.strftime('%Y-%m-%d')}",
            amount=balance_amount,
            currency="USD",  # WSECU only deals with USD accounts
            type="BALANCE",
            source_account=account_number,
        )

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from WSECU CSV file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        # Count total rows for reversed_lineno calculation
        self.input_file.seek(0)
        row_count_reader = csv.DictReader(self.input_file)
        row_count = sum(1 for _ in row_count_reader)

        # Process transactions
        self.input_file.seek(0)
        reader = csv.DictReader(self.input_file)
        
        for i, row in enumerate(reader):
            try:
                # Parse transaction data - do date parsing first to catch invalid dates early
                transaction_date = parse_date(row["Date"])
                amount = decimal.Decimal(row["Amount"])
                description = row["Description"].strip()
                category = row["Category"].strip() if row["Category"].strip() else None
                note = row["Note"].strip() if row["Note"].strip() else None
                account_number = row["Account number"]
                
                # Create extra dict for any unused fields
                extra = {}
                balance = row["Balance"].strip()
                if balance:
                    extra["balance"] = balance

                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=i + 1,
                    reversed_lineno=i - row_count,
                    date=transaction_date,
                    post_date=transaction_date,  # WSECU doesn't separate transaction and post dates
                    desc=description,
                    amount=amount,
                    currency="USD",  # WSECU only deals with USD accounts
                    category=category,
                    note=note,
                    source_account=account_number,
                    extra=extra if extra else None,
                )
            except (ValueError, KeyError, decimal.InvalidOperation, IndexError) as e:
                # Skip malformed transactions but continue processing
                continue

        # Add balance transaction using the most recent date
        most_recent_row = self._find_most_recent_transaction()
        if most_recent_row:
            yield self._create_balance_transaction(most_recent_row, filename)
