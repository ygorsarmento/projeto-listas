"""
================================================================================
NORMALIZADOR E MESCLADOR DE COLUNAS
================================================================================

Mescla múltiplas colunas em uma única conforme regras de transformação.

FLUXO:
1. Carrega configuração de mesclagem
2. Encontra colunas usando busca fuzzy
3. Mescla colunas conforme regras
4. Exporta resultado
"""

import pandas as pd
import os
import unicodedata
from difflib import get_close_matches
from pipeline.config import ARQUIVO_MESCLAGEM
from pipeline.config import ARQUIVO_MESCLADO_CSV
from pipeline.extract.xlsx_extractor import encontrar_coluna_similar, remover_acentos
from pipeline.extract.utils import ler_excel_robusto


def normalizar_colunas_duplicadas(df):
    """
    Normaliza nomes de colunas tratando duplicatas.
    Adiciona sufixo _1, _2, etc para colunas duplicadas.
    """
    from collections import Counter
    
    cols = [col.strip() for col in df.columns]
    counts = Counter(cols)
    duplicates = {col: count for col, count in counts.items() if count > 1}
    
    if duplicates:
        col_count = {}
        new_cols = []
        
        for col in cols:
            if col in duplicates:
                col_count[col] = col_count.get(col, 0) + 1
                new_cols.append(f"{col}_{col_count[col]}")
            else:
                new_cols.append(col)
        
        df.columns = new_cols
    
    return df


def carregar_config_mesclagem(arquivo_config=None):
    """
    Carrega arquivo de configuração de mesclagem.

    Retorna DataFrame com colunas:
    - nome_da_coluna_mesclada
    - colunas_para_mesclagem
    """
    arquivo_config = arquivo_config or ARQUIVO_MESCLAGEM
    
    if not os.path.exists(arquivo_config):
        print(f"⚠️  Arquivo de configuração não encontrado: {arquivo_config}")
        return pd.DataFrame()
    
    try:
        # usa leitura robusta
        df = ler_excel_robusto(arquivo_config)

        # normaliza nomes das colunas
        df.columns = [remover_acentos(str(col).strip().lower()) for col in df.columns]

        # normaliza também os valores dentro das colunas de mesclagem
        for col in df.columns:
            df[col] = df[col].apply(lambda x: remover_acentos(str(x).strip()) if pd.notna(x) else x)

        return df
    except Exception as e:
        print(f"✗ Erro ao carregar configuração: {e}")
        return pd.DataFrame()


def construir_dict_mesclagem(df_config):
    """
    Constrói dicionário de mesclagem a partir da configuração.
    
    Retorna:
    {
        "nome_coluna_mesclada": ["coluna_origem_1", "coluna_origem_2", ...]
    }
    """
    colunas_para_mesclar = {}
    
    # Encontrar nomes de coluna na config
    nome_col_mesclada = None
    nome_col_origem = None
    
    for col in df_config.columns:
        if "nome_da_coluna_mesclada" in col or "nome da coluna mesclada" in col:
            nome_col_mesclada = col
        if "colunas_para_mesclagem" in col or "colunas para mesclagem" in col:
            nome_col_origem = col
    
    if not nome_col_mesclada or not nome_col_origem:
        print("⚠️  Configuração inválida. Esperado colunas: 'nome_da_coluna_mesclada', 'colunas_para_mesclagem'")
        return {}
    
    for index, row in df_config.iterrows():
        coluna_mesclada = str(row[nome_col_mesclada]).strip()
        coluna_origem = str(row[nome_col_origem]).strip()
        
        # Tratar sequências de escape
        try:
            coluna_origem = coluna_origem.encode('utf-8').decode('unicode_escape')
        except:
            pass
        
        # Normalizar com NFD
        try:
            nfd = unicodedata.normalize('NFD', coluna_origem)
            coluna_origem = ''.join(
                char for char in nfd
                if unicodedata.category(char) != 'Mn'
            )
        except:
            pass
        
        if coluna_mesclada not in colunas_para_mesclar:
            colunas_para_mesclar[coluna_mesclada] = []
        
        colunas_para_mesclar[coluna_mesclada].append(coluna_origem)
    
    return colunas_para_mesclar


def mesclar_colunas(df_consolidado, colunas_para_mesclar):
    """
    Executa a mesclagem de colunas conforme configuração.
    
    Para cada regra de mesclagem:
    - Encontra colunas usando busca fuzzy
    - Concatena valores com espaço
    - Remove duplicatas
    """
    df_mesclado = pd.DataFrame()
    
    print("\n" + "─"*80)
    print("PROCESSAMENTO: Mesclando colunas")
    print("─"*80 + "\n")
    
    # Normalizar DataFrame consolidado
    df_consolidado = normalizar_colunas_duplicadas(df_consolidado)
    
    # Para cada coluna a ser mesclada, encontrar colunas correspondentes e mesclar
    for nome_coluna_mesclada, colunas_origem in colunas_para_mesclar.items():
        
        # Encontrar colunas disponíveis
        colunas_existentes = []
        
        # Para cada coluna de origem, encontrar a coluna correspondente no DataFrame consolidado
        for col_origem in colunas_origem:
            col_encontrada = encontrar_coluna_similar(
                col_origem,
                df_consolidado.columns
            )
            
            if col_encontrada:
                colunas_existentes.append(col_encontrada)
        
        # Se encontrou colunas, mesclar
        if colunas_existentes:
            df_mesclado[nome_coluna_mesclada] = df_consolidado[colunas_existentes].apply(
    lambda x: ' '.join(
        dict.fromkeys(
            remover_acentos(str(val)).strip().replace("\n", " ").replace("\r", " ")
            for val in x.dropna()
        )
    ),
    axis=1
)
            
            print(f"  ✓ '{nome_coluna_mesclada}'")
            print(f"    ← Mesclagem de {len(colunas_existentes)} coluna(s):")
            for col_encontrada in colunas_existentes:
                print(f"       • {col_encontrada}")
            print()
        
        else:
            print(f"  ✗ '{nome_coluna_mesclada}' ← ERRO: NENHUMA COLUNA ENCONTRADA")
            print(f"    Colunas procuradas:")
            for col_origem in colunas_origem:
                print(f"       • '{col_origem}'")
            print(f"    Dica: Verificar se os nomes estão corretos na configuração.\n")
    
    return df_mesclado


def run(df_consolidado, arquivo_config=None):
    """
    Executa o pipeline completo de mesclagem.
    
    Args:
        df_consolidado: DataFrame consolidado
        arquivo_config: Arquivo de configuração
        
    Returns:
        DataFrame mesclado
    """
    print("\n" + "="*60)
    print("📊 ETAPA 3: Mesclagem de Colunas")
    print("="*60)
    
    # Carregar configuração
    print(f"\n📋 Carregando arquivo de configuração...")
    df_config = carregar_config_mesclagem(arquivo_config)
    
    if df_config.empty:
        print("⚠️  Nenhuma configuração de mesclagem disponível")
        return df_consolidado
    
    print(f"✓ Configuração carregada: {len(df_config)} regra(s)")
    
    # Construir dicionário
    colunas_para_mesclar = construir_dict_mesclagem(df_config)
    
    if not colunas_para_mesclar:
        print("⚠️  Nenhuma regra de mesclagem processada")
        return df_consolidado
    
    print(f"✓ {len(colunas_para_mesclar)} coluna(s) a serem criadas:")
    for nome in colunas_para_mesclar.keys():
        print(f"  • '{nome}'")
    
    # Mesclar
    df_mesclado = mesclar_colunas(df_consolidado, colunas_para_mesclar)
    
    print(f"\n✓ Etapa 3 concluída!")
    print(f"  → Total de colunas mescladas: {len(df_mesclado)}")
    
    # Exportar resultado como dataframe mesclado
    df_mesclado.to_csv(ARQUIVO_MESCLADO_CSV, index=False)
    print(f"✓ Resultado exportado: {ARQUIVO_MESCLADO_CSV}")


if __name__ == "__main__":
    from pipeline.extract.xlsx_extractor import run as extract_xlsx
    
    df = extract_xlsx()
    df_mesclado = run(df)
    print(df_mesclado)
