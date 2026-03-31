"""
Comparar os arquivos filtrados por "VERDADEIRO" e "FALSO" na Coluna1.
"""

import pandas as pd

from pipeline.config import DIRETORIO_SAIDA_XLSX_UPDATE, ARQUIVO_LISTAS_FAMILIAS_ATUALIZADAS
from pipeline.extract.utils import ler_excel_robusto


def limpar_cpf(cpf):
    """Mantém apenas os 11 primeiros dígitos numéricos do CPF e retorna (cpf, precisou_completar)."""
    if pd.isna(cpf):
        return cpf, False
    cpf_original = ''.join(filter(str.isdigit, str(cpf)))[:11]
    precisou_completar = len(cpf_original) < 11 and len(cpf_original) > 0
    return cpf_original.zfill(11) if cpf_original else cpf_original, precisou_completar


def preparar_df(df):
    """Formata datas e CPF para padronização."""
    df = df.copy()
    for col in ['DT_SEI', 'DT_ENVIO', 'DT_NASC_RF']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d/%m/%Y')
    df[['CPF_RF', 'CPF_COM_ZEROS']] = df['CPF_RF'].apply(limpar_cpf).tolist()
    return df


def normalizar_colunas(df, colunas):
    """Coloca colunas de texto em maiúsculas, retirar os acentos e caracteres especiais."""
    for col in colunas:
        df[col] = df[col].str.upper().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    return df


def comparar_e_exportar(df_false, df_true, on, subset, ordenar_por, arquivo_saida):
    """Compara dois dataframes e exporta as diferenças."""
    diferenca = pd.merge(
        df_false,
        df_true,
        on=on,
        how='outer',
        indicator=True,
        suffixes=('_false', '_true')
    )
    diferenca = diferenca[diferenca['_merge'] != 'both']
    diferenca = diferenca.sort_values(ordenar_por, ascending=False)
    diferenca = diferenca.drop_duplicates(subset=subset, keep='first', ignore_index=True)
    diferenca.to_excel(f"{DIRETORIO_SAIDA_XLSX_UPDATE}\\{arquivo_saida}", index=False)
    print(f"Diferenças exportadas: {arquivo_saida} ({len(diferenca)} registros)")
    return diferenca


# Carregar e preparar os dados
listas_tratadas = ler_excel_robusto(ARQUIVO_LISTAS_FAMILIAS_ATUALIZADAS, sheet_name='dados_consolidados_enviados')
listas_tratadas = preparar_df(listas_tratadas)

# Filtrar por Coluna1
true = listas_tratadas.query('Coluna1 == True')
false = listas_tratadas.query('Coluna1 == False')

# CPFs inválidos para filtragem
cpfs_invalidos = ['00000000001', '00000000002']

# --- Comparação por NOME_RF (sem CPF válido) ---
false_sem_cpf = normalizar_colunas(
    false[false['CPF_RF'].isna() | false['CPF_RF'].isin(cpfs_invalidos)].copy(),
    ['NOME_RF', 'NOME_MAE']
)
true_sem_cpf = normalizar_colunas(
    true[true['CPF_RF'].isna() | true['CPF_RF'].isin(cpfs_invalidos)].copy(),
    ['NOME_RF', 'NOME_MAE']
)

diferenca_nome = comparar_e_exportar(
    false_sem_cpf,
    true_sem_cpf,
    on='NOME_RF',
    subset=['NOME_RF', 'NOME_MAE_false', 'CNUC_false'],
    ordenar_por='DT_ENVIO_false',
    arquivo_saida='Diferenca_True_False_NOME.xlsx'
)

# --- Comparação por CPF_RF (com CPF válido) ---
false_com_cpf = normalizar_colunas(
    false[false['CPF_RF'].notnull() & ~false['CPF_RF'].isin(cpfs_invalidos)].copy(),
    ['NOME_RF', 'NOME_MAE']
)
true_com_cpf = normalizar_colunas(
    true[true['CPF_RF'].notnull() & ~true['CPF_RF'].isin(cpfs_invalidos)].copy(),
    ['NOME_RF', 'NOME_MAE']
)

diferenca_cpf = comparar_e_exportar(
    false_com_cpf,
    true_com_cpf,
    on='CPF_RF',
    subset=['CPF_RF', 'NOME_RF_false', 'NOME_MAE_false', 'CNUC_false'],
    ordenar_por='DT_ENVIO_false',
    arquivo_saida='Diferenca_True_False_CPF.xlsx'
)
