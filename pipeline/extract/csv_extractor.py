"""
================================================================================
EXTRATOR DE DADOS CSV
================================================================================

Extrai e consolida dados de múltiplos arquivos CSV.

FLUXO:
1. Lista arquivos e suas colunas
2. Extrai dados de todos os arquivos
3. Consolida em um único DataFrame
4. Adiciona coluna com nome da origem
"""

import pandas as pd
import glob
import os

from pipeline.config import CAMINHO_CSV, ARQUIVO_CONSOLIDADO_CSV, ARQUIVO_COLUNAS_CSV
from pipeline.extract.utils import ler_csv_robusto, normalizar_colunas, adicionar_coluna_origem


def listar_arquivos_e_colunas(caminho):
    """
    Lista todos os arquivos CSV e suas respectivas colunas.
    
    Args:
        caminho (str): Caminho da pasta com arquivos CSV
        
    Returns:
        dict: Dicionário com {nome_arquivo: [colunas_normalizadas]}
    """
    arquivos = glob.glob(f"{caminho}/*.csv")
    resultado = {}
    
    for arquivo in arquivos:
        nome_arquivo = os.path.basename(arquivo)
        try:
            colunas = ler_csv_robusto(arquivo, nrows=0, dtype=str).columns.tolist()
            resultado[nome_arquivo] = normalizar_colunas(colunas)
        except Exception as e:
            print(f"⚠️  Erro ao ler colunas de {nome_arquivo}: {e}")
    
    return resultado


def extrair_e_consolidar_csv(caminho):
    """
    Extrai todas as colunas e consolida dados de todos os arquivos CSV como string.
    
    Args:
        caminho (str): Caminho da pasta com arquivos CSV
        
    Returns:
        pd.DataFrame: DataFrame consolidado com todos os dados como string
    """
    arquivos = glob.glob(f"{caminho}/*.csv")
    dataframes = []
    
    for arquivo in arquivos:
        nome_arquivo = os.path.basename(arquivo)
        try:
            df = ler_csv_robusto(arquivo, dtype=str)
            df.columns = normalizar_colunas(df.columns)
            df = adicionar_coluna_origem(df, nome_arquivo)
            dataframes.append(df)
            print(f"✓ Extraído: {nome_arquivo} ({len(df)} linhas)")
        except Exception as e:
            print(f"✗ Erro ao processar {nome_arquivo}: {e}")
    
    if not dataframes:
        raise ValueError("Nenhum arquivo CSV foi processado com sucesso!")
    
    df_consolidado = pd.concat(dataframes, ignore_index=True)
    df_consolidado = df_consolidado.astype(str)  # Garante que todas as colunas são string
    return df_consolidado


def run(caminho=None, salvar=True):
    """
    Executa o pipeline completo de extração CSV.
    
    Args:
        caminho (str, optional): Caminho da pasta. Se None, usa CAMINHO_CSV
        salvar (bool): Se deve salvar os arquivos
        
    Returns:
        tuple: (df_colunas, df_consolidado)
    """
    caminho = caminho or CAMINHO_CSV
    
    print("\n" + "="*60)
    print("📊 ETAPA 1: Listando arquivos e colunas CSV")
    print("="*60)
    
    # Listar arquivos e colunas
    colunas_por_arquivo = listar_arquivos_e_colunas(caminho)
    df_colunas = pd.DataFrame(
        list(colunas_por_arquivo.items()),
        columns=["arquivo", "colunas"]
    )
    print(f"\n✓ Encontrados {len(df_colunas)} arquivos CSV")
    
    #if salvar:
        #df_colunas.to_csv(ARQUIVO_COLUNAS_CSV, index=False)
        #print(f"✓ Salvo: {ARQUIVO_COLUNAS_CSV}")
    
    print("\n" + "="*60)
    print("📊 ETAPA 2: Extraindo e consolidando dados CSV")
    print("="*60)
    
    # Extrair e consolidar dados
    df_consolidado = extrair_e_consolidar_csv(caminho)
    print(f"\n✓ Total de linhas consolidadas: {len(df_consolidado)}")
    
    if salvar:
        df_consolidado.to_csv(ARQUIVO_CONSOLIDADO_CSV, index=False)
        print(f"✓ Salvo: {ARQUIVO_CONSOLIDADO_CSV}")
    
    return df_colunas, df_consolidado


if __name__ == "__main__":
    run()
