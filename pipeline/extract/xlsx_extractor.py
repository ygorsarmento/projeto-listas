"""
================================================================================
EXTRATOR DE DADOS XLSX
================================================================================

Extrai e consolida dados de múltiplas planilhas Excel.

FLUXO:
1. Lista arquivos e suas planilhas
2. Extrai dados de planilhas filtradas
3. Consolida em um único DataFrame
"""

import pandas as pd
import glob
import os
import unicodedata
import re
from difflib import get_close_matches

from pipeline.config import CAMINHO_XLSX, ARQUIVO_COLUNAS_XLSX, ARQUIVO_CONSOLIDADO_XLSX, ARQUIVO_FILTRO_XLSX
from pipeline.config import ARQUIVO_FILTRO_XLSX
from pipeline.extract.utils import ler_excel_robusto, normalizar_colunas, remover_acentos


def encontrar_coluna_similar(coluna_procurada, colunas_disponiveis, threshold=0.8):
    """
    Encontra coluna pelo nome mesmo com variações de acentos ou ortografia.
    
    Usa 3 níveis de busca:
    1. Match exato
    2. Match sem acentos
    3. Fuzzy matching
    """
    coluna_procurada = str(coluna_procurada)
    
    # Nível 1: Match exato
    if coluna_procurada in colunas_disponiveis:
        return coluna_procurada
    
    # Nível 2: Match sem acentos (case-insensitive)
    coluna_sem_acento = remover_acentos(coluna_procurada)
    for col_disp in colunas_disponiveis:
        if remover_acentos(col_disp) == coluna_sem_acento:
            return col_disp
    
    # Nível 3: Fuzzy matching
    colunas_sem_acento = [remover_acentos(c) for c in colunas_disponiveis]
    matches = get_close_matches(
        coluna_sem_acento,
        colunas_sem_acento,
        n=1,
        cutoff=0.6
    )
    
    # Se encontrar um match fuzzy, retorna a coluna original correspondente 
    if matches:
        for col_disp in colunas_disponiveis:
            if remover_acentos(col_disp) == matches[0]:
                return col_disp
    
    return None

# Função para listar arquivos e planilhas
def listar_arquivos_e_planilhas(caminho):
    """
    Lista todos os arquivos XLSX e suas respectivas planilhas, bem como o número de linhas em cada uma.
    """
    arquivos = glob.glob(f"{caminho}/*.xlsx")
    dados = []
    
    for arquivo in arquivos:
        try:
            xls = pd.ExcelFile(arquivo)
            nome_arquivo = os.path.basename(arquivo)
            
            for sheet in xls.sheet_names:
                dados.append({
                    "arquivo": nome_arquivo,
                    "planilha": sheet,
                    "linhas": len(pd.read_excel(xls, sheet_name=sheet, dtype=str))
                })
        except Exception as e:
            print(f"✗ Erro ao ler {arquivo}: {e}")
    
    return dados


def extrair_dados_xlsx(caminho, arquivo_filtro):
    """
    Extrai dados de planilhas filtradas ou de todas as planilhas.

    Args:
        caminho: Caminho com arquivos XLSX
        arquivo_filtro: Arquivo de configuração ou DataFrame já carregado
    """

    # Lê arquivo de filtro ou usa DataFrame já carregado
    if isinstance(arquivo_filtro, pd.DataFrame):
        print(f"\n📋 Usando DataFrame já carregado ({len(arquivo_filtro)} planilhas)")
        df_config = arquivo_filtro
    elif arquivo_filtro and os.path.exists(arquivo_filtro):
        print(f"\n📋 Usando arquivo de filtro: {arquivo_filtro}")
        df_config = ler_excel_robusto(arquivo_filtro, dtype=str)
        df_config = df_config.rename(columns=str.lower)
    else:
        print(f"\n📋 Processando todas as planilhas encontradas")
        dados = listar_arquivos_e_planilhas(caminho)
        df_config = pd.DataFrame(dados)
    
    dataframes = []
    
    # Itera sobre cada linha do arquivo de configuração
    for index, row in df_config.iterrows():
        arquivo = row.get("arquivo", row.get("Arquivo", ""))
        planilha = row.get("planilha", row.get("Planilha", ""))
        linhas = row.get("linhas", row.get("Linhas", 0))
        
        caminho_arquivo = os.path.join(caminho, arquivo)

       # Tenta ler a planilha, normalizar colunas e adicionar colunas de origem 
        try:
            df = ler_excel_robusto(caminho_arquivo, sheet_name=planilha, dtype=str)
            df.columns = normalizar_colunas(df.columns)
            df["arquivo"] = arquivo
            df["planilha"] = planilha
            df["linhas"] = linhas
            
            dataframes.append(df)
            print(f"  ✓ Extraído: {arquivo} → Planilha '{planilha}' ({len(df)} linhas)")
            
        except FileNotFoundError:
            print(f"  ✗ Arquivo não encontrado: {caminho_arquivo}")
        except Exception as e:
            print(f"  ✗ Erro ao ler {arquivo}/{planilha}: {e}")
    
    if not dataframes:
        raise ValueError("Nenhuma planilha foi processada com sucesso!")
    
    # Concat funciona automaticamente - colunas diferentes viram NaN
    df_consolidado = pd.concat(dataframes, ignore_index=True)
    df_consolidado = df_consolidado.astype(str)
    
    return df_consolidado


def run(caminho=None, arquivo_filtro=None, salvar=True):
    """
    Executa o pipeline completo de extração XLSX.
    
    Args:
        caminho: Caminho da pasta com XLSX. Se None, usa CAMINHO_XLSX
        arquivo_filtro: Arquivo de configuração com planilhas a processar
        salvar: Se deve salvar os arquivos
    """
    caminho = caminho or CAMINHO_XLSX
    arquivo_filtro = arquivo_filtro or ARQUIVO_FILTRO_XLSX

    print("\n" + "="*60)
    print("ETAPA 1: Verificando arquivo de filtro XLSX")
    print("="*60)

    # Verifica se existe arquivo de filtro
    if arquivo_filtro and os.path.exists(arquivo_filtro):
        print(f"\n📋 Usando arquivo de filtro: {arquivo_filtro}")
        df_config = ler_excel_robusto(arquivo_filtro, dtype=str)
        df_config = df_config.rename(columns=str.lower)
        print(f"✓ Encontradas {len(df_config)} planilha(s) no arquivo de filtro")
    else:
        # Se não existe filtro, lista todas as planilhas disponíveis
        print(f"\n⚠️  Arquivo de filtro não encontrado: {arquivo_filtro}")
        print(f"📋 Listando todas as planilhas disponíveis...")
        
        dados = listar_arquivos_e_planilhas(caminho)
        df_planilhas = pd.DataFrame(dados)
        print(f"✓ Encontradas {len(df_planilhas)} planilha(s)")

        if salvar:
            # Exportar lista para arquivo de filtro
            df_planilhas.to_excel(ARQUIVO_COLUNAS_XLSX, index=False)
            print(f"✓ Salvo: {ARQUIVO_COLUNAS_XLSX}")
            print(f"💡 Edite este arquivo para selecionar quais planilhas processar")
        
        df_config = df_planilhas
    
    print("\n" + "="*60)
    print("ETAPA 2: Extraindo e consolidando dados XLSX")
    print("="*60)
    
    # Extrair dados usando o df_config já carregado
    df_consolidado = extrair_dados_xlsx(caminho, df_config)
    print(f"\n✓ Total de linhas consolidadas: {len(df_consolidado)}")
    
    if salvar:
        df_consolidado.to_excel(ARQUIVO_CONSOLIDADO_XLSX, index=False)
    print(f"✓ Salvo: {ARQUIVO_CONSOLIDADO_XLSX}")
    
    return df_consolidado


if __name__ == "__main__":
    run()
