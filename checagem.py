"""
Comparar os arquivos filtrados por "VERDADEIRO" e "FALSO" na Coluna1.
Gera uma única planilha com todos os registros e colunas indicadoras de correspondência.
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


def comparar_e_exportar_completo(df_false, df_true, arquivo_saida):
    """
    Compara dois dataframes por CPF e NOME, mantendo TODOS os registros (false + true).
    Adiciona colunas indicadoras de correspondência e duplicidade.
    """
    df_false = df_false.copy().reset_index(drop=True)
    df_true = df_true.copy().reset_index(drop=True)
    
    # Prepara colunas para unificar estrutura
    todas_colunas = sorted(set(df_false.columns) | set(df_true.columns))
    
    # Padroniza colunas do false com sufixo _false
    df_false_pad = df_false.copy()
    df_false_pad.columns = [f'{c}_false' if c not in ['MATCH_CPF', 'MATCH_NOME', 'Duplicado_FALSE'] else c 
                            for c in df_false_pad.columns]
    
    # Padroniza colunas do true com sufixo _true
    df_true_pad = df_true.copy()
    df_true_pad.columns = [f'{c}_true' if c not in ['MATCH_CPF', 'MATCH_NOME', 'Duplicado_FALSE'] else c 
                           for c in df_true_pad.columns]
    
    # Concatena todos os registros
    resultado = pd.concat([df_false_pad, df_true_pad], ignore_index=True)
    
    # Marca origem
    resultado['ORIGEM'] = ['FALSE'] * len(df_false_pad) + ['TRUE'] * len(df_true_pad)
    resultado['Duplicado_FALSE'] = False
    resultado['MATCH_CPF'] = False
    resultado['MATCH_NOME'] = False
    
    # Marca duplicatas no FALSE
    mask_false = resultado['ORIGEM'] == 'FALSE'
    resultado.loc[mask_false, 'Duplicado_FALSE'] = resultado.loc[mask_false].duplicated(subset=['CPF_RF_false'], keep=False)
    
    # Busca matches por CPF (FALSE -> TRUE)
    true_por_cpf = df_true.drop_duplicates(subset=['CPF_RF'], keep='first').set_index('CPF_RF')
    
    for idx in resultado[resultado['ORIGEM'] == 'FALSE'].index:
        cpf = resultado.loc[idx, 'CPF_RF_false']
        if pd.notna(cpf) and cpf != '' and cpf in true_por_cpf.index:
            resultado.loc[idx, 'MATCH_CPF'] = True
            resultado.loc[idx, 'MATCH_NOME'] = True
    
    # Busca matches por NOME (FALSE -> TRUE, apenas sem match por CPF)
    sem_match_cpf = resultado[(resultado['ORIGEM'] == 'FALSE') & (resultado['MATCH_CPF'] == False)]
    true_por_nome = df_true.drop_duplicates(subset=['NOME_RF'], keep='first').set_index('NOME_RF')
    
    for idx in sem_match_cpf.index:
        nome = resultado.loc[idx, 'NOME_RF_false']
        if pd.notna(nome) and nome != '' and nome in true_por_nome.index:
            resultado.loc[idx, 'MATCH_NOME'] = True
    
    # Remove coluna auxiliar e ordena
    resultado = resultado.drop(columns=['ORIGEM'])
    
    if 'DT_ENVIO_false' in resultado.columns:
        resultado = resultado.sort_values('DT_ENVIO_false', ascending=False)
    elif 'DT_ENVIO_true' in resultado.columns:
        resultado = resultado.sort_values('DT_ENVIO_true', ascending=False)
    
    # Exporta
    resultado.to_excel(f"{DIRETORIO_SAIDA_XLSX_UPDATE}\\{arquivo_saida}", index=False)
    print(f"Comparação exportada: {arquivo_saida} ({len(resultado)} registros)")
    print(f"  - Duplicados no FALSE: {resultado['Duplicado_FALSE'].sum()}")
    print(f"  - Match por CPF: {resultado['MATCH_CPF'].sum()}")
    print(f"  - Match por NOME (sem CPF): {resultado['MATCH_NOME'].sum() - resultado['MATCH_CPF'].sum()}")
    print(f"  - Sem correspondência: {len(resultado) - resultado['MATCH_NOME'].sum()}")
    
    return resultado


# Carregar e preparar os dados
listas_tratadas = ler_excel_robusto(ARQUIVO_LISTAS_FAMILIAS_ATUALIZADAS, sheet_name='dados_consolidados_enviados')
listas_tratadas = preparar_df(listas_tratadas)

# Filtrar por Coluna1
true = listas_tratadas.query('Coluna1 == True').copy()
false = listas_tratadas.query('Coluna1 == False').copy()

# CPFs inválidos para filtragem
cpfs_invalidos = ['00000000001', '00000000002']

# Normaliza colunas de texto em ambos os dataframes
colunas_normalizar = ['NOME_RF', 'NOME_MAE']
true = normalizar_colunas(true, colunas_normalizar)
false = normalizar_colunas(false, colunas_normalizar)

# Compara TODOS os registros (CPF válido e inválido juntos)
# O merge por CPF vai capturar os matches por CPF automaticamente
# CPFs nulos/inválidos não farão match por CPF, mas farão por NOME
resultado_final = comparar_e_exportar_completo(
    false,
    true,
    arquivo_saida='Comparacao_Completa_Consolidada.xlsx'
)

print(f"\nTotal: {len(resultado_final)} registros")
print(f"  - Match CPF: {resultado_final['MATCH_CPF'].sum()}")
print(f"  - Match NOME: {resultado_final['MATCH_NOME'].sum()}")
