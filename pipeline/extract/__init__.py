"""
Módulo de Extração (Extract)

Funções para extração de dados de múltiplas fontes:
- CSV: csv_extractor
- XLSX: xlsx_extractor
- Utilidades: utils
"""

from pipeline.extract.csv_extractor import run as extract_csv
from pipeline.extract.xlsx_extractor import run as extract_xlsx
from pipeline.extract.utils import ler_csv_robusto, ler_excel_robusto, normalizar_colunas

__all__ = [
    'extract_csv',
    'extract_xlsx',
    'ler_csv_robusto',
    'ler_excel_robusto',
    'normalizar_colunas'
]
