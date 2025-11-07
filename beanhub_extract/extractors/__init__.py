import os
import typing

from .ally_bank_ofx import AllyBankOFXExtractor
from .bac_san_jose_bank import BacSanJoseBankExtractor
from .bac_san_jose_credit import BacSanJoseCreditExtractor
from .banco_bcr import BancoBcrExtractor
from .banco_nacional import BancoNacionalExtractor
from .base import ExtractorBase
from .capital_one_ofx import CapitalOneOFXExtractor
from .chase import ChaseCreditCardExtractor
from .credit_human import CreditHumanExtractor
from .credit_human_ofx import CreditHumanOFXExtractor
from .csv import CSVExtractor
from .docfcu import DocfcuExtractor
from .etrade_ofx import EtradeOFXExtractor
from .lfcu_ofx import LfcuOFXExtractor
from .mercury import MercuryExtractor
from .plaid import PlaidExtractor
from .synchrony_pdf import SynchronyPdfExtractor
from .ussfcu_ofx import UssfcuOFXExtractor
from .wealthsimple import WealthsimpleExtractor
from .wsecu import WSECUExtractor

ALL_EXTRACTORS: dict[str, typing.Type[ExtractorBase]] = {
    AllyBankOFXExtractor.EXTRACTOR_NAME: AllyBankOFXExtractor,
    BacSanJoseBankExtractor.EXTRACTOR_NAME: BacSanJoseBankExtractor,
    BacSanJoseCreditExtractor.EXTRACTOR_NAME: BacSanJoseCreditExtractor,
    BancoBcrExtractor.EXTRACTOR_NAME: BancoBcrExtractor,
    BancoNacionalExtractor.EXTRACTOR_NAME: BancoNacionalExtractor,
    MercuryExtractor.EXTRACTOR_NAME: MercuryExtractor,
    ChaseCreditCardExtractor.EXTRACTOR_NAME: ChaseCreditCardExtractor,
    CapitalOneOFXExtractor.EXTRACTOR_NAME: CapitalOneOFXExtractor,
    CreditHumanExtractor.EXTRACTOR_NAME: CreditHumanExtractor,
    CreditHumanOFXExtractor.EXTRACTOR_NAME: CreditHumanOFXExtractor,
    DocfcuExtractor.EXTRACTOR_NAME: DocfcuExtractor,
    EtradeOFXExtractor.EXTRACTOR_NAME: EtradeOFXExtractor,
    LfcuOFXExtractor.EXTRACTOR_NAME: LfcuOFXExtractor,
    PlaidExtractor.EXTRACTOR_NAME: PlaidExtractor,
    SynchronyPdfExtractor.EXTRACTOR_NAME: SynchronyPdfExtractor,
    UssfcuOFXExtractor.EXTRACTOR_NAME: UssfcuOFXExtractor,
    WealthsimpleExtractor.EXTRACTOR_NAME: WealthsimpleExtractor,
    WSECUExtractor.EXTRACTOR_NAME: WSECUExtractor,
    CSVExtractor.EXTRACTOR_NAME: CSVExtractor,
}


def detect_extractor(input_file: typing.TextIO) -> typing.Type[ExtractorBase]:
    for extractor_cls in ALL_EXTRACTORS.values():
        input_file.seek(os.SEEK_SET)
        if extractor_cls(input_file).detect():
            return extractor_cls
