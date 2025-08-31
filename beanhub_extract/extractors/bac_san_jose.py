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
    """Parse DD/MM/YYYY date format used by BAC San Jose."""
    parts = date_str.split("/")
    return datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))


class BacSanJoseExtractor(ExtractorBase):
    EXTRACTOR_NAME = "bac_san_jose"
    DEFAULT_IMPORT_ID = "bac_san_jose:{{ source_account }}:{{ reference }}"
    
    # Expected header for account info section (we only use first 4 fields)
    ACCOUNT_HEADER = [
        "Número de Clientes", "Nombre", "Producto", "Moneda"
    ]
    
    # Expected header for transaction details section
    TRANSACTION_HEADER = [
        "Fecha de Transacción", "Referencia de Transacción", "Código de Transacción", 
        "Descripción de Transacción", "Débito de Transacción", "Crédito de Transacción", 
        "Balance de Transacción"
    ]

    def detect(self) -> bool:
        """Detect if this is a BAC San Jose CSV file."""
        try:
            self.input_file.seek(0)
            content = self.input_file.read()
            self.input_file.seek(0)
            
            # Check for key Spanish headers that identify BAC San Jose format
            return (
                "Detalle de Estado Bancario" in content and
                "Fecha de Transacción" in content and
                "Referencia de Transacción" in content and
                "Código de Transacción" in content
            )
        except Exception:
            return False

    def fingerprint(self) -> Fingerprint | None:
        """Generate fingerprint based on account info and last transaction's balance."""
        try:
            account_info, transactions = self._parse_file_sections()
            if not account_info or not transactions:
                return None
            
            # Find the last transaction for fingerprint (like we do for balance)
            last_transaction = self._find_last_transaction(transactions)
            if not last_transaction:
                return None
            
            # Use account info and last transaction's balance for fingerprint
            hash_obj = hashlib.sha256()
            hash_obj.update(account_info["account_number"].encode("utf8"))
            hash_obj.update(last_transaction["Balance de Transacción"].encode("utf8"))
            hash_obj.update(last_transaction["Fecha de Transacción"].encode("utf8"))
            hash_obj.update(last_transaction["Referencia de Transacción"].encode("utf8"))
            
            return Fingerprint(
                starting_date=parse_date(last_transaction["Fecha de Transacción"]),
                first_row_hash=hash_obj.hexdigest(),
            )
        except Exception:
            return None

    def _parse_file_sections(self) -> tuple[dict | None, list[dict]]:
        """Parse the BAC CSV file into account info and transactions sections."""
        self.input_file.seek(0)
        lines = self.input_file.readlines()
        
        account_info = None
        transactions = []
        
        # Find account info section (line 2 contains the account data)
        if len(lines) >= 2:
            try:
                # Parse account info from line 2
                account_line = lines[1].strip()
                account_parts = [part.strip() for part in account_line.split(',')]
                
                if len(account_parts) >= 4:
                    account_info = {
                        "customer_number": account_parts[0],
                        "name": account_parts[1],
                        "account_number": account_parts[2],
                        "currency": account_parts[3],
                    }
            except Exception:
                pass
        
        # Find transaction section (starts after "Detalle de Estado Bancario")
        transaction_start = None
        for i, line in enumerate(lines):
            if "Fecha de Transacción" in line:
                transaction_start = i + 1
                break
        
        if transaction_start:
            # Parse transactions until we hit summary section or empty line
            for i in range(transaction_start, len(lines)):
                line = lines[i].strip()
                if not line or "Resumen de Estado Bancario" in line:
                    break
                
                try:
                    parts = [part.strip() for part in line.split(',')]
                    if len(parts) >= 7:
                        transactions.append({
                            "Fecha de Transacción": parts[0],
                            "Referencia de Transacción": parts[1],
                            "Código de Transacción": parts[2],
                            "Descripción de Transacción": parts[3],
                            "Débito de Transacción": parts[4],
                            "Crédito de Transacción": parts[5],
                            "Balance de Transacción": parts[6],
                        })
                except Exception:
                    continue
        
        return account_info, transactions

    def _find_last_transaction(self, transactions: list[dict]) -> dict | None:
        """Find the last transaction in the file for balance purposes.
        BAC CSV transactions are ordered chronologically, and the last transaction 
        in the file represents the final balance regardless of same-day ordering.
        """
        if not transactions:
            return None
        
        # Simply return the last transaction in the file
        # This handles same-day transactions in the correct order
        return transactions[-1]

    def _create_balance_transaction(self, last_transaction: dict, account_info: dict, filename: str | None) -> Transaction:
        """Create a balance transaction from the last transaction's running balance."""
        # Use the running balance from the last transaction
        balance_amount = decimal.Decimal(last_transaction["Balance de Transacción"])
        
        # Parse the last transaction date and add one day for balance assertion
        last_transaction_date = parse_date(last_transaction["Fecha de Transacción"])
        balance_date = last_transaction_date + datetime.timedelta(days=1)
        
        account_number = account_info["account_number"]
        
        return Transaction(
            extractor=self.EXTRACTOR_NAME,
            file=filename,
            lineno=0,  # Balance transaction doesn't correspond to a specific line
            transaction_id=f"BALANCE_{last_transaction_date.strftime('%Y%m%d')}_{account_number}",
            date=balance_date,
            post_date=balance_date,
            desc=f"Balance as of {last_transaction_date.strftime('%Y-%m-%d')}",
            amount=balance_amount,
            currency=account_info["currency"],
            type="BALANCE",
            source_account=account_number,
        )

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from BAC San Jose CSV file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        account_info, transactions = self._parse_file_sections()
        
        if not account_info:
            return
        
        account_number = account_info["account_number"]
        
        # Process each transaction
        for i, txn_row in enumerate(transactions):
            try:
                # Parse transaction data
                transaction_date = parse_date(txn_row["Fecha de Transacción"])
                transaction_ref = txn_row["Referencia de Transacción"]
                transaction_code = txn_row["Código de Transacción"]
                description = txn_row["Descripción de Transacción"].strip()
                
                # Parse amounts - BAC uses separate debit/credit columns
                debit_str = txn_row["Débito de Transacción"].strip()
                credit_str = txn_row["Crédito de Transacción"].strip()
                
                # Determine transaction amount (negative for debits, positive for credits)
                if debit_str and debit_str != "0.00":
                    amount = -decimal.Decimal(debit_str)
                elif credit_str and credit_str != "0.00":
                    amount = decimal.Decimal(credit_str)
                else:
                    continue  # Skip transactions with no amount
                
                # Create extra dict for additional fields (keep running_balance for debugging)
                extra = {
                    "running_balance": txn_row["Balance de Transacción"],
                }

                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=i + 1,
                    reversed_lineno=i - len(transactions),
                    date=transaction_date,
                    post_date=transaction_date,  # BAC doesn't separate transaction and post dates
                    desc=description,
                    amount=amount,
                    currency=account_info["currency"],
                    source_account=account_number,
                    reference=transaction_ref,
                    category=transaction_code,  # Use category field for transaction code
                    extra=extra,
                )
            except (ValueError, KeyError, decimal.InvalidOperation, IndexError) as e:
                # Skip malformed transactions but continue processing
                continue

        # Add balance transaction using the last transaction chronologically
        if transactions and account_info:
            # Find the chronologically last transaction
            last_transaction = self._find_last_transaction(transactions)
            if last_transaction:
                yield self._create_balance_transaction(last_transaction, account_info, filename)
