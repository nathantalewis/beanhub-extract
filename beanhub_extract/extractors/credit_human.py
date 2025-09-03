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
    """Parse MM/DD/YY date format."""
    parts = date_str.split("/")
    year = int(parts[2])
    # Handle 2-digit years - assume 20xx for years < 50, 19xx for years >= 50
    if year < 50:
        year += 2000
    else:
        year += 1900
    return datetime.date(year, int(parts[0]), int(parts[1]))


def parse_currency_amount(amount_str: str) -> decimal.Decimal:
    """Parse currency amount like '$194.46' or '$49,969.15'."""
    # Remove dollar sign, quotes, and commas
    cleaned = amount_str.replace('"', '').replace('$', '').replace(',', '')
    return decimal.Decimal(cleaned)


class CreditHumanExtractor(ExtractorBase):
    EXTRACTOR_NAME = "credit_human"
    DEFAULT_IMPORT_ID = "credit_human:{{ source_account }}:{{ transaction_id }}"
    ALL_FIELDS = [
        "Account ID",
        "Transaction ID", 
        "Date",
        "Description",
        "Check Number",
        "Category",
        "Tags",
        "Amount",
        "Balance",
    ]

    def detect(self) -> bool:
        """Detect if this is a Credit Human CSV file."""
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
        Since Credit Human CSV is ordered with most recent transactions first, 
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
        balance_amount = parse_currency_amount(balance_row["Balance"])
        account_id = balance_row["Account ID"]
        
        return Transaction(
            extractor=self.EXTRACTOR_NAME,
            file=filename,
            lineno=0,  # Balance transaction doesn't correspond to a specific line
            transaction_id=f"BALANCE_{transaction_date.strftime('%Y%m%d')}_{account_id}",
            date=balance_date,
            post_date=balance_date,
            desc=f"Balance as of {transaction_date.strftime('%Y-%m-%d')}",
            amount=balance_amount,
            currency="USD",  # Credit Human deals with USD accounts
            type="BALANCE",
            source_account=account_id,
        )

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from Credit Human CSV file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        # Process transactions
        self.input_file.seek(0)
        reader = csv.DictReader(self.input_file)
        
        for i, row in enumerate(reader):
            try:
                # Parse transaction data - do date parsing first to catch invalid dates early
                transaction_date = parse_date(row["Date"])
                amount = parse_currency_amount(row["Amount"])
                description = row["Description"].strip()
                transaction_id = row["Transaction ID"].strip()
                account_id = row["Account ID"].strip()
                
                # Optional fields
                check_number = row["Check Number"].strip() if row["Check Number"].strip() else None
                category = row["Category"].strip() if row["Category"].strip() else None
                tags = row["Tags"].strip() if row["Tags"].strip() else None
                
                # Create extra dict for any unused fields
                extra = {}
                balance = row["Balance"].strip()
                if balance:
                    extra["balance"] = balance
                if check_number:
                    extra["check_number"] = check_number
                if tags:
                    extra["tags"] = tags

                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=i + 1,
                    transaction_id=transaction_id,
                    date=transaction_date,
                    post_date=transaction_date,  # Credit Human doesn't separate transaction and post dates
                    desc=description,
                    amount=amount,
                    currency="USD",  # Credit Human deals with USD accounts
                    category=category,
                    source_account=account_id,
                    extra=extra if extra else None,
                )
            except (ValueError, KeyError, decimal.InvalidOperation, IndexError) as e:
                # Skip malformed transactions but continue processing
                continue

        # Add balance transaction using the most recent date
        most_recent_row = self._find_most_recent_transaction()
        if most_recent_row:
            yield self._create_balance_transaction(most_recent_row, filename)
