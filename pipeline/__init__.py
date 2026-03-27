"""
Pipeline ETL - Extração, Transformação e Carregamento de dados

Módulos:
- extract: Extração de dados (CSV, XLSX)
- transform: Transformação e normalização
- load: Carregamento de dados
"""

from . import extract
from . import transform
from . import load

__all__ = ['extract', 'transform', 'load']
