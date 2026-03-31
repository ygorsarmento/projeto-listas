""" 
Esse código tem como objetivo comparar os arquivos filtrados por "VERDADEIRO" e "FALSO" na Coluna1.

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
listas_tratadas['DT_SEI'] = pd.to_datetime(listas_tratadas['DT_SEI'], errors='coerce').dt.strftime('%d/%m/%Y')
listas_tratadas['DT_ENVIO'] = pd.to_datetime(listas_tratadas['DT_ENVIO'], errors='coerce').dt.strftime('%d/%m/%Y')
listas_tratadas['DT_NASC_RF'] = pd.to_datetime(listas_tratadas['DT_NASC_RF'], errors='coerce').dt.strftime('%d/%m/%Y')

# Formata a coluna CPF_RF como string para preservar os zeros à esquerda
listas_tratadas['CPF_RF'] = listas_tratadas['CPF_RF'].astype(str)

# Filtrar as linhas onde a Coluna1 é "VERDADEIRO" usando .query
listas_filtradas_true = listas_tratadas.query('Coluna1 == True')
listas_filtradas_false = listas_tratadas.query('Coluna1 == False')

print("Linhas filtradas onde Coluna1 é VERDADEIRO:")
print(listas_filtradas_true)

print("Linhas filtradas onde Coluna1 é FALSO:")
print(listas_filtradas_false)


# Primeiro, trata os casos em que CPF_RF está vazio ou com 00000000001 e 00000000002 para comparação po NOME_RF, coloca NOME_RF em maiúsculas, depois compara por NOME_RF para os casos sem CPF_RF
false_sem_cpf = listas_filtradas_false[listas_filtradas_false['CPF_RF'].isna() | (listas_filtradas_false['CPF_RF'] == '00000000001') | (listas_filtradas_false['CPF_RF'] == '00000000002')]
true_sem_cpf = listas_filtradas_true[listas_filtradas_true['CPF_RF'].isna() | (listas_filtradas_true['CPF_RF'] == '00000000001') | (listas_filtradas_true['CPF_RF'] == '00000000002')]
false_sem_cpf['NOME_RF'] = false_sem_cpf['NOME_RF'].str.upper()
true_sem_cpf['NOME_RF'] = true_sem_cpf['NOME_RF'].str.upper()
false_sem_cpf['NOME_MAE'] = false_sem_cpf['NOME_MAE'].str.upper()
true_sem_cpf['NOME_MAE'] = true_sem_cpf['NOME_MAE'].str.upper()

diferenca_por_nome = pd.merge(
    false_sem_cpf,
    true_sem_cpf,
    on='NOME_RF',
    how='outer',
    indicator=True,
    suffixes=('_false', '_true')
)

# Ordena false por data de envio, mantém apenas os diferentes e retira as duplicatas por NOME_RF, NOME_MAE, CNUC
diferenca_por_nome = diferenca_por_nome[diferenca_por_nome['_merge'] != 'both']
diferenca_por_nome = diferenca_por_nome.sort_values('DT_ENVIO_false', ascending=False)
diferenca_por_nome = diferenca_por_nome.drop_duplicates(subset=['NOME_RF', 'NOME_MAE_false', 'CNUC_false'], keep='first', ignore_index=True)


# Mantém apenas as linhas que estão em um DataFrame mas não no outro
diferenca_por_nome = diferenca_por_nome[diferenca_por_nome['_merge'] != 'both']

# Exporta as diferenças por NOME_RF para um novo arquivo Excel
diferenca_por_nome.to_excel(f"{DIRETORIO_SAIDA_XLSX_UPDATE}\\Diferenca_True_False_NOME.xlsx", index=False)
print("Diferenças por NOME_RF exportadas com sucesso para Diferenca_True_False_NOME.xlsx")

"""# Retira os CPF_RF vazios ou com 00000000001 e 00000000002 para comparação por CPF
listas_filtradas_false = listas_filtradas_false[listas_filtradas_false['CPF_RF'].notnull() & (listas_filtradas_false['CPF_RF'] != '00000000001') & (listas_filtradas_false['CPF_RF'] != '00000000002')]
listas_filtradas_true = listas_filtradas_true[listas_filtradas_true['CPF_RF'].notnull() & (listas_filtradas_true['CPF_RF'] != '00000000001') & (listas_filtradas_true['CPF_RF'] != '00000000002')]
listas_filtradas_true = listas_filtradas_true.apply(lambda x: x.str.upper() if x.name in ['NOME_RF', 'NOME_MAE'] else x)
listas_filtradas_false = listas_filtradas_false.apply(lambda x: x.str.upper() if x.name in ['NOME_RF', 'NOME_MAE'] else x)


# Comparação inicial por CPF_RF
diferenca_true_false = pd.merge(
    listas_filtradas_false,
    listas_filtradas_true,
    on='CPF_RF',
    how='outer',
    indicator=True,
    suffixes=('_false', '_true')
)

# Ordena false por data de envio, mantém apenas os diferentes e retira as duplicatas por CPF_RF, NOME_RF, NOME_MAE, DT_NASC_RF, CNUC
diferenca_true_false = diferenca_true_false[diferenca_true_false['_merge'] != 'both']
diferenca_true_false = diferenca_true_false.sort_values('DT_ENVIO_false', ascending=False)
diferenca_true_false = diferenca_true_false.drop_duplicates(subset=['CPF_RF', 'NOME_RF_false', 'NOME_MAE_false', 'CNUC_false'], keep='first', ignore_index=True)

print("Diferenças entre as linhas filtradas por VERDADEIRO e FALSO:")
print(diferenca_true_false)

# Exportar as diferenças para um novo arquivo Excel
diferenca_true_false.to_excel(f"{DIRETORIO_SAIDA_XLSX_UPDATE}\\Diferenca_True_False_CPF.xlsx", index=False)
print("Diferenças exportadas com sucesso para Diferenca_True_False_CPF.xlsx")"""

