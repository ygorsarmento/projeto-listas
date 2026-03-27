"""
Módulo de Transformação (Transform)

Funções para transformação e normalização de dados:
- Mesclagem de colunas: normalizer
"""

from .normalizer import run as mesclar_colunas
from .normalizer import construir_dict_mesclagem, carregar_config_mesclagem

__all__ = [
    'mesclar_colunas',
    'construir_dict_mesclagem',
    'carregar_config_mesclagem'
]
