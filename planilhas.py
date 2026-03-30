""" 
Esse código tem como objetivo criar uma planilha excel para cada Arquivo filtrado por "VERDADEIRO" na Coluna1.

"""

import pandas as pd

from pipeline.config import DIRETORIO_SAIDA_XLSX_UPDATE, ARQUIVO_LISTAS_FAMILIAS_ATUALIZADAS
from pipeline.extract.utils import ler_excel_robusto

# Carregar as listas de famílias tratadas e atualizadas
listas_tratadas = ler_excel_robusto(ARQUIVO_LISTAS_FAMILIAS_ATUALIZADAS, sheet_name='dados_consolidados_enviados')

#Listas colunas presente no dataframe
def colunas_presentes(df):
    return set(df.columns)

# Obter as colunas presentes em cada DataFrame
colunas_tratadas = colunas_presentes(listas_tratadas)

print("Colunas presentes em listas_tratadas:")
print(colunas_tratadas)

# Formata a coluna DT_SEI, DT_Envio e DT_NASC_RF para o formato de data dd/mm/yyyy
listas_tratadas['DT_SEI'] = pd.to_datetime(listas_tratadas['DT_SEI'], errors='coerce').dt.strftime('%d-%m-%Y')
listas_tratadas['DT_ENVIO'] = pd.to_datetime(listas_tratadas['DT_ENVIO'], errors='coerce').dt.strftime('%d-%m-%Y')
listas_tratadas['DT_NASC_RF'] = pd.to_datetime(listas_tratadas['DT_NASC_RF'], errors='coerce').dt.strftime('%d-%m-%Y')

# Formata a coluna CPF_RF como string para preservar os zeros à esquerda
listas_tratadas['CPF_RF'] = listas_tratadas['CPF_RF'].astype(str).str.zfill(11)

# Filtrar as linhas onde a Coluna1 é "VERDADEIRO" usando .query
listas_filtradas = listas_tratadas.query('Coluna1 == True')

print("Linhas filtradas onde Coluna1 é VERDADEIRO:")
print(listas_filtradas)

# Exporta o DataFrame filtrado para um novo arquivo Excel
listas_filtradas.to_excel("C:\\Users\\ygors\\OneDrive - ICMBio\\Equipe PBV - Listas de Famílias\\Listas consolidadas\\Atualização_20260327\\Listas_Filtradas.xlsx", index=False)
print("Listas filtradas exportadas com sucesso para Listas_Filtradas.xlsx")

# Cria uma planilha excel para cada combinação de NOME_UC e DT_ENVIO de por "VERDADEIRO" na Coluna1 e retira a Coluna1 da planilha final
for (nome_uc, dt_envio), group in listas_filtradas.groupby(['NOME_UC', 'DT_ENVIO']):
    group.drop(columns=['Coluna1'], inplace=True)
    group.to_excel(f"{DIRETORIO_SAIDA_XLSX_UPDATE}\\{nome_uc}_{dt_envio}.xlsx", index=False)
    print(f"Planilha {nome_uc}_{dt_envio}.xlsx criada com sucesso.")

