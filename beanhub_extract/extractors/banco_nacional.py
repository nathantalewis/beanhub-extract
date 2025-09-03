import csv
import datetime
import decimal
import hashlib
import typing

from ..data_types import Fingerprint
from ..data_types import Transaction
from .base import ExtractorBase


def parse_date(date_str: str) -> datetime.date:
    """Parse DD/MM/YYYY date format used by Banco Nacional."""
    parts = date_str.strip().split("/")
    return datetime.date(int(parts[2]), int(parts[1]), int(parts[0]))


class BancoNacionalExtractor(ExtractorBase):
    EXTRACTOR_NAME = "banco_nacional"
    DEFAULT_IMPORT_ID = "banco_nacional:{{ transaction_id }}"

    def detect(self) -> bool:
        """Detect if this is a Banco Nacional CSV file."""
        try:
            self.input_file.seek(0)
            content = self.input_file.read()
            self.input_file.seek(0)
            
            # Check for semicolon-separated format and Spanish headers
            lines = content.strip().split('\n')
            if not lines:
                return False
                
            header = lines[0]
            return (
                ";" in header and
                "oficina" in header.lower() and
                "fechaMovimiento" in header and
                "numeroDocumento" in header and
                "debito" in header.lower() and
                "credito" in header.lower() and
                "descripcion" in header.lower()
            )
        except Exception:
            return False

    def fingerprint(self) -> Fingerprint | None:
        """Generate fingerprint based on first transaction."""
        try:
            transactions = self._parse_transactions()
            if not transactions:
                return None
                
            first_transaction = transactions[0]
            
            # Create hash from first transaction data
            hash_obj = hashlib.sha256()
            hash_obj.update(str(first_transaction["oficina"]).encode("utf8"))
            hash_obj.update(first_transaction["fechaMovimiento"].encode("utf8"))
            hash_obj.update(first_transaction["numeroDocumento"].encode("utf8"))
            hash_obj.update(first_transaction["descripcion"].encode("utf8"))
            
            return Fingerprint(
                starting_date=parse_date(first_transaction["fechaMovimiento"]),
                first_row_hash=hash_obj.hexdigest(),
            )
        except Exception:
            return None

    def _parse_transactions(self) -> list[dict]:
        """Parse transactions from the CSV file."""
        self.input_file.seek(0)
        
        # Use csv.reader with semicolon delimiter
        reader = csv.DictReader(self.input_file, delimiter=';')
        transactions = []
        
        for row in reader:
            # Skip total/summary rows
            if not row.get("fechaMovimiento") or row.get("fechaMovimiento").strip() == "":
                continue
            if "TOTAL" in row.get("numeroDocumento", "").upper():
                continue
                
            # Clean up the row data
            cleaned_row = {}
            for key, value in row.items():
                if key and value is not None:
                    cleaned_row[key.strip()] = value.strip()
            
            if cleaned_row:
                transactions.append(cleaned_row)
        
        return transactions

    def __call__(self) -> typing.Generator[Transaction, None, None]:
        """Extract transactions from Banco Nacional CSV file."""
        filename = None
        if hasattr(self.input_file, "name"):
            filename = self.input_file.name

        transactions = self._parse_transactions()
        
        if not transactions:
            return
            
        # Process each transaction
        for i, txn_row in enumerate(transactions):
            try:
                # Parse transaction data
                transaction_date = parse_date(txn_row["fechaMovimiento"])
                oficina = txn_row["oficina"]
                numero_documento = txn_row["numeroDocumento"]
                descripcion = txn_row["descripcion"]
                
                # Parse amounts - Banco Nacional uses separate debit/credit columns
                debito_str = txn_row.get("debito", "").strip()
                credito_str = txn_row.get("credito", "").strip()
                
                # Determine transaction amount (negative for debits, positive for credits)
                amount = None
                if debito_str and debito_str != "0.00" and debito_str != "":
                    amount = -decimal.Decimal(debito_str)
                elif credito_str and credito_str != "0.00" and credito_str != "":
                    amount = decimal.Decimal(credito_str)
                else:
                    continue  # Skip transactions with no amount
                
                # Create extra dict for additional fields
                extra = {
                    "oficina": oficina,
                }

                yield Transaction(
                    extractor=self.EXTRACTOR_NAME,
                    file=filename,
                    lineno=i + 2,  # +2 because CSV has header row and we're 0-indexed
                    reversed_lineno=len(transactions) - i,
                    transaction_id=numero_documento,
                    date=transaction_date,
                    post_date=transaction_date,  # The Banco Nacional export doesn't have separate post dates
                    desc=descripcion,
                    amount=amount,
                    currency=None,
                    source_account=None,
                    reference=numero_documento,
                    extra=extra,
                )
                
            except (ValueError, KeyError, decimal.InvalidOperation, IndexError):
                # Skip malformed transactions but continue processing
                continue
