""" 
Esse código tem como objetivo atualizar as listas de famílias que já foram tratadas.

"""

import pandas as pd

from pipeline.config import ARQUIVO_LISTAS_FAMILIAS_TRATADAS, ARQUIVO_LISTAS_FAMILIAS_ATUALIZADAS, DIRETORIO_SAIDA_XLSX_UPDATE
from pipeline.extract.utils import ler_excel_robusto


# Carregar as listas de famílias tratadas e atualizadas
#
listas_tratadas = ler_excel_robusto(ARQUIVO_LISTAS_FAMILIAS_TRATADAS, sheet_name='dados_consolidados_enviados')
listas_atualizadas = ler_excel_robusto(ARQUIVO_LISTAS_FAMILIAS_ATUALIZADAS, sheet_name='Atualização')

# Função para obter as colunas presentes em um DataFrame
def colunas_presentes(df):
    return set(df.columns)

# Obter as colunas presentes em cada DataFrame
colunas_tratadas = colunas_presentes(listas_tratadas)
colunas_atualizadas = colunas_presentes(listas_atualizadas)

print("Colunas presentes em listas_tratadas:")
print(colunas_tratadas)

print("Colunas presentes em listas_atualizadas:")
print(colunas_atualizadas)

# Fução para identificar colunas que estão em um DataFrame mas não no outro
def colunas_diferentes(df1, df2):
    colunas_df1 = colunas_presentes(df1)
    colunas_df2 = colunas_presentes(df2)
    
    colunas_somente_df1 = colunas_df1 - colunas_df2
    colunas_somente_df2 = colunas_df2 - colunas_df1
    
    return colunas_somente_df1, colunas_somente_df2

# Identificar colunas que estão em listas_tratadas mas não em listas_atualizadas, e vice-versa
colunas_somente_tratadas, colunas_somente_atualizadas = colunas_diferentes(listas_tratadas, listas_atualizadas)

print("Colunas presentes em listas_tratadas mas não em listas_atualizadas:")
print(colunas_somente_tratadas)

# Acrescentar os dois DataFrames usando as colunas comuns mantendo as colunas exclusivas de cada um
colunas_comuns = colunas_tratadas.intersection(colunas_atualizadas)
listas_combinadas = pd.concat([listas_tratadas[[colunas_comuns]], listas_atualizadas[[colunas_comuns]]], ignore_index=True)

# Exportar o DataFrame combinado para um novo arquivo Excel
#listas_combinadas.to_excel(f"{DIRETORIO_SAIDA_XLSX_UPDATE}\\Listas_Combinadas.xlsx", index=False)

print("Listas combinadas exportadas com sucesso para Listas_Combinadas.xlsx")
