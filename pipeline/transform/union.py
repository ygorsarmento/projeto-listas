"""
================================================================================
União de DataFrames com base em configuração de mesclagem
================================================================================

Este arquivo é responsável por unir os arquivos CSV extraídos e processados, 
utilizando as configurações definidas no arquivo excel de mesclagem. 
Ele garante que o tratamento dos nomes das colunas seja consistente e que os dados sejam combinados corretamente,
mesmo quando os nomes das colunas diferem entre os arquivos de origem.

Fluxo geral:
1. Carregar os arquivos CSV Consolidado e Mesclado.
2. Carregar a configuração de mesclagem.
3. Normalizar os nomes das colunas dos DataFrames para facilitar a mesclagem.
4. Realizar a mesclagem dos DataFrames com base na configuração.
5. Exportar o DataFrame mesclado para um arquivo CSV.

"""

import pandas as pd

from pipeline.config import ARQUIVO_MESCLAGEM_FINAL, ARQUIVO_MESCLADO_CSV, ARQUIVO_CONSOLIDADO_CSV, ENCODINGS, SEPARADORES_CSV, CAMINHO_CSV
from pipeline.extract.utils import ler_csv_robusto


# ===============================================================================
# 1. Carregar os arquivos CSV consolidado e mesclado
# ===============================================================================

# Dicionário de arquivos a carregar
arquivos = {
    "consolidado": ARQUIVO_CONSOLIDADO_CSV,
    "mesclado": ARQUIVO_MESCLADO_CSV
}

# Função para carregar arquivos CSV de forma robusta
dfs = {}
for chave, caminho in arquivos.items():
    df = ler_csv_robusto(caminho)
    if df is not None:
        dfs[chave] = df
    else:
        print(f"⚠️  Aviso: Não foi possível carregar o arquivo '{caminho}' para '{chave}'.")

# ===============================================================================
# 2. Carregar a configuração de mesclagem
# ===============================================================================

# Carregar configuração de mesclagem
try:
    config_mesclagem = pd.read_excel(ARQUIVO_MESCLAGEM_FINAL, dtype=str)
    print(f"✅ Configuração de mesclagem carregada com sucesso: {ARQUIVO_MESCLAGEM_FINAL}")

except Exception as e:
    print(f"❌ Erro ao carregar configuração de mesclagem: {e}")
    config_mesclagem = pd.DataFrame()  # DataFrame vazio para evitar erros posteriores



