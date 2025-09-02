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
    """Parse DD/MM/YYYY date format used by BAC San Jose credit cards."""
    parts = date_str.split("/")
    return datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))


class BacSanJoseCreditExtractor(ExtractorBase):
    EXTRACTOR_NAME = "bac_san_jose_credit"
    DEFAULT_IMPORT_ID = "bac_san_jose_credit:{{ last_four_digits }}:{{ transaction_id }}"
    
    def detect(self) -> bool:
        """Detect if this is a BAC San Jose credit card CSV file."""
        try:
            self.input_file.seek(0)
            content = self.input_file.read()
            self.input_file.seek(0)
            
            # Check for key headers that identify BAC San Jose credit card format
            return (
                "Pro000000000000duct" in content and
                "Minimum payment/due date" in content and
                "Cash payment/Due date" in content and
                "Date, , Local, Dollars" in content
            )
        except Exception:
            return False

    def fingerprint(self) -> Fingerprint | None:
        """Generate fingerprint based on account info from the second line."""
        try:
            account_info = self._parse_file_sections()
            
            # Use account info from second line for fingerprint
            hash_obj = hashlib.sha256()
            hash_obj.update(account_info["source_account"].encode("utf8"))
            hash_obj.update(account_info["statement_date"].encode("utf8"))
            hash_obj.update(account_info["cash_payment_local"].encode("utf8"))
            hash_obj.update(account_info["cash_payment_dollars"].encode("utf8"))
            
            return Fingerprint(
                starting_date=parse_date(account_info["statement_date"]),
                first_row_hash=hash_obj.hexdigest(),
            )
        except ValueError:
            # If we can't parse the file structure, we can't generate a fingerprint
            return None
        except Exception:
            # Unexpected errors during fingerprint generation should return None
            # The file might be partially corrupted or have unexpected format
            return None

    def _parse_file_sections(self) -> dict:
        """Parse the BAC credit card CSV file and return account info.
        
        Returns:
            Dictionary containing account information from CSV line 2
            
        Raises:
            ValueError: If the file format is invalid or required fields are missing.
        """
        self.input_file.seek(0)
        lines = self.input_file.readlines()
        
        # Parse account info from first two lines - this is critical for processing
        if len(lines) < 2:
            raise ValueError("BAC San José credit card CSV must have at least 2 lines")
        
        try:
            # Line 2 contains account summary info
            account_line = lines[1].strip()
            account_parts = [part.strip() for part in account_line.split(',')]
            
            if len(account_parts) < 9:
                raise ValueError(f"Expected at least 9 fields in account info line, got {len(account_parts)}")
            
            # Extract last 4 digits from main card number for source account
            full_card_number = account_parts[0]
            if not full_card_number:
                raise ValueError("Card number field is empty in account info")
            
            source_account = full_card_number.split('-')[-1] if '-' in full_card_number else full_card_number[-4:]
            
            # Validate required fields
            statement_date = account_parts[2]
            if not statement_date:
                raise ValueError("Statement date field is empty in account info")
            
            cash_payment_local = account_parts[7]
            cash_payment_dollars = account_parts[8]
            
            account_info = {
                "source_account": source_account,
                "statement_date": statement_date,
                "cash_payment_local": cash_payment_local,
                "cash_payment_dollars": cash_payment_dollars,
            }
        except (IndexError, ValueError) as e:
            raise ValueError(f"Failed to parse BAC San José credit card account info: {e}") from e
        
        return account_info

    def _parse_transactions(self) -> list[dict]:
        """Parse transactions from the BAC credit card CSV file.
        
        Returns:
            List of transaction dictionaries
            
        Raises:
            ValueError: If transaction section is malformed or transactions cannot be parsed
        """
        self.input_file.seek(0)
        lines = self.input_file.readlines()
        
        transactions = []
        
        # Find transaction section (starts after "Date, , Local, Dollars")
        transaction_start = None
        for i, line in enumerate(lines):
            if "Date, , Local, Dollars" in line:
                transaction_start = i + 1
                break
        
        if transaction_start is None:
            raise ValueError("Could not find transaction section header 'Date, , Local, Dollars' in CSV")
        
        current_last_four = None
        
        # Parse transactions until we hit summary section
        for i in range(transaction_start, len(lines)):
            line = lines[i].strip()
            if not line or "CURRENT Interest MONTH" in line:
                break
            
            try:
                parts = [part.strip() for part in line.split(',')]
                if len(parts) < 4:
                    raise ValueError(f"Transaction line {i+1} has insufficient fields: expected at least 4, got {len(parts)}")
                
                date_part = parts[0]
                desc_part = parts[1]
                local_amount = parts[2]
                dollar_amount = parts[3]
                
                # Check if this is a card number grouping line (no date, contains card pattern with asterisks)
                if (not date_part and 
                    '*' in desc_part and 
                    '-' in desc_part and
                    local_amount == "0.00" and 
                    dollar_amount == "0.00"):
                    # Extract last 4 digits from card number (format: ****-**-****-NNNN)
                    current_last_four = desc_part.split('-')[-1] if '-' in desc_part else desc_part[-4:]
                    continue
                
                # Skip non-transaction lines (no date or special entries)
                if not date_part or not date_part.replace("/", "").isdigit():
                    continue
                
                # Skip zero-amount transactions
                if local_amount in ["0.00", ""] and dollar_amount in ["0.00", ""]:
                    continue
                
                # Validate that we have a current card context
                if current_last_four is None:
                    raise ValueError(f"Transaction on line {i+1} found before any card grouping line")
                
                # Determine transaction amount and currency - fail if neither has a valid amount
                amount = None
                currency = None
                
                if local_amount and local_amount != "0.00":
                    try:
                        # Negate the amount to match Beancount liability account convention
                        amount = -decimal.Decimal(local_amount)
                        currency = "CRC"
                    except decimal.InvalidOperation as e:
                        raise ValueError(f"Invalid local amount '{local_amount}' on line {i+1}") from e
                elif dollar_amount and dollar_amount != "0.00":
                    try:
                        # Negate the amount to match Beancount liability account convention
                        amount = -decimal.Decimal(dollar_amount)
                        currency = "USD"
                    except decimal.InvalidOperation as e:
                        raise ValueError(f"Invalid dollar amount '{dollar_amount}' on line {i+1}") from e
                
                if amount is not None and currency is not None:
                    # Validate date format
                    try:
                        parse_date(date_part)  # This will raise ValueError if invalid
                    except (ValueError, IndexError) as e:
                        raise ValueError(f"Invalid date format '{date_part}' on line {i+1}") from e
                    
                    transactions.append({
                        "date": date_part,
                        "description": desc_part,
                        "amount": amount,
                        "currency": currency,
                        "last_four_digits": current_last_four,
                        "line_number": i + 1,
                    })
            except ValueError:
                # Re-raise ValueError with context
                raise
            except Exception as e:
                # Convert unexpected errors to ValueError with context
                raise ValueError(f"Failed to parse transaction on line {i+1}: {e}") from e
        
        return transactions

    def _create_balance_transactions(self, account_info: dict, 
                                   filename: str | None) -> list[Transaction]:
        """Create balance transactions from cash payment amounts (one for each currency if non-zero).
        
        Args:
            account_info: Dictionary containing account information from CSV line 2
            filename: Optional filename for the transaction
            
        Returns:
            List of balance transactions (one per currency with non-zero amounts)
            
        Raises:
            ValueError: If required account info fields are missing or invalid
            decimal.InvalidOperation: If cash payment amounts are not valid decimal numbers
        """
        transactions = []
        
        # Validate required fields
        if "statement_date" not in account_info or not account_info["statement_date"]:
            raise ValueError("Statement date is required for balance transactions")
        if "source_account" not in account_info or not account_info["source_account"]:
            raise ValueError("Source account is required for balance transactions")
        
        # Parse statement date for balance transaction
        try:
            statement_date = parse_date(account_info["statement_date"])
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid statement date format: {account_info['statement_date']}") from e
        
        # Source account is already the last 4 digits
        source_account = account_info["source_account"]
        last_four_digits = source_account
        
        # Create CRC balance transaction if amount is non-zero
        cash_payment_local = account_info["cash_payment_local"]
        if cash_payment_local and cash_payment_local != "0.00":
            # For liability accounts in Beancount, amounts owed should be negative
            balance_amount_crc = -decimal.Decimal(cash_payment_local)
            
            transactions.append(Transaction(
                extractor=self.EXTRACTOR_NAME,
                file=filename,
                lineno=0,  # Balance transaction doesn't correspond to a specific line
                transaction_id=f"BALANCE_{statement_date.strftime('%Y%m%d')}_{last_four_digits}_CRC",
                date=statement_date,
                post_date=statement_date,
                desc=f"Balance as of {statement_date.strftime('%Y-%m-%d')} (CRC)",
                amount=balance_amount_crc,
                currency="CRC",
                type="BALANCE",
                source_account=source_account,
                last_four_digits=last_four_digits,
            ))
        
        # Create USD balance transaction if amount is non-zero
        cash_payment_dollars = account_info["cash_payment_dollars"]
        if cash_payment_dollars and cash_payment_dollars != "0.00":
            # For liability accounts in Beancount, amounts owed should be negative
            balance_amount_usd = -decimal.Decimal(cash_payment_dollars)
            
            transactions.append(Transaction(
                extractor=self.EXTRACTOR_NAME,
                file=filename,
                lineno=0,  # Balance transaction doesn't correspond to a specific line
                transaction_id=f"BALANCE_{statement_date.strftime('%Y%m%d')}_{last_four_digits}_USD",
                date=statement_date,
                post_date=statement_date,
                desc=f"Balance as of {statement_date.strftime('%Y-%m-%d')} (USD)",
                amount=balance_amount_usd,
                currency="USD",
                type="BALANCE",
                source_account=source_account,
                last_four_digits=last_four_digits,
            ))
        
        return transactions

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from BAC San Jose credit card CSV file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        account_info = self._parse_file_sections()
        
        # Use the source account from line 2
        source_account = account_info["source_account"]
        
        # Parse transactions from the CSV
        transactions = self._parse_transactions()
        
        # Process each transaction - fail on any malformed transaction
        for i, txn_row in enumerate(transactions):
            # Parse transaction data - all validation was done in _parse_transactions
            transaction_date = parse_date(txn_row["date"])
            description = txn_row["description"].strip()
            amount = txn_row["amount"]
            currency = txn_row["currency"]
            last_four_digits = txn_row["last_four_digits"]
            line_number = txn_row["line_number"]
            
            # Generate a simple transaction ID
            transaction_id = f"{transaction_date.strftime('%Y%m%d')}_{i}"
            
            # Create extra dict for additional fields
            extra = {}

            yield Transaction(
                extractor=self.EXTRACTOR_NAME,
                file=filename,
                lineno=line_number,
                reversed_lineno=i - len(transactions),
                transaction_id=transaction_id,
                date=transaction_date,
                post_date=transaction_date,
                desc=description,
                amount=amount,
                currency=currency,
                source_account=source_account,
                last_four_digits=last_four_digits,
                extra=extra,
            )
        
        # Add balance transactions from cash payment amounts
        try:
            balance_transactions = self._create_balance_transactions(account_info, filename)
            for balance_txn in balance_transactions:
                yield balance_txn
        except (ValueError, decimal.InvalidOperation) as e:
            # Balance transaction creation failed - this is important to know about
            raise ValueError(f"Failed to create balance transactions: {e}") from e
