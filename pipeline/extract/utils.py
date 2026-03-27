"""
================================================================================
UTILIDADES DE EXTRAÇÃO
================================================================================

Funções compartilhadas para leitura de arquivos com tratamento robusto de
encoding e normalização de colunas.
"""

import pandas as pd
import re
import datetime
from pipeline.config import ENCODINGS, SEPARADORES_CSV
import unicodedata

def remover_acentos(texto: str) -> str:
    """Remove acentos e normaliza para comparação."""
    try:
        texto = str(texto)
        # normaliza para NFD (separa acentos)
        nfd = unicodedata.normalize("NFD", texto)
        # remove caracteres de acento (Mn = Mark, Nonspacing)
        texto_sem_acento = ''.join(
            ch for ch in nfd if unicodedata.category(ch) != 'Mn'
        )
        return texto_sem_acento.strip()
    except Exception:
        return str(texto).strip()



def ler_csv_robusto(arquivo, **kwargs):
    """
    Lê arquivo CSV com tratamento robusto de encoding e separadores.
    
    Tenta múltiplas combinações de encoding e separadores até conseguir ler.
    
    Args:
        arquivo (str): Caminho do arquivo CSV
        **kwargs: Argumentos adicionais para pd.read_csv()
        
    Returns:
        pd.DataFrame: DataFrame com os dados
        
    Raises:
        ValueError: Se nenhuma combinação de encoding/separador funcionar
    """
    for encoding in ENCODINGS:
        for separador in SEPARADORES_CSV:
            try:
                return pd.read_csv(arquivo, encoding=encoding, sep=separador, **kwargs)
            except (UnicodeDecodeError, UnicodeError, pd.errors.ParserError):
                continue
    
    raise ValueError(
        f"Não foi possível ler o arquivo {arquivo} com nenhuma combinação "
        f"de encoding/separador disponível. Encodings tentados: {ENCODINGS}. "
        f"Separadores tentados: {SEPARADORES_CSV}"
    )


def ler_excel_robusto(arquivo, sheet_name=0, **kwargs):
    """
    Lê arquivo Excel com tratamento de erros e normaliza os dados:
    - Remove caracteres invisíveis (\r, \n, \t, apóstrofo)
    - Substitui vírgula por ponto
    - Remove espaços extras
    - Converte CPFs para numérico limpo (se possível)
    """
    try:
        df = pd.read_excel(arquivo, sheet_name=sheet_name, **kwargs)

        # Função de limpeza robusta
        def limpar_texto(x):
            if pd.isna(x):
                return x
            # Preserva datetime e outros tipos não-texto
            if isinstance(x, (int, float, pd.Timestamp, datetime.datetime)):
                return x
            x = str(x)
            x = re.sub(r"^[']+", "", x)          # remove apóstrofo inicial
            x = re.sub(r"[\r\n\t]", " ", x)      # remove quebras invisíveis
            x = x.replace(",", ".")              # troca vírgula por ponto
            return x.strip()

        # Limpa nomes das colunas
        df.columns = [limpar_texto(c) for c in df.columns]

        # Limpa conteúdo das células de texto
        for col in df.select_dtypes(include=["object", "string"]).columns:
            df[col] = df[col].apply(limpar_texto)

        # Limpa CPFs (remove caracteres não-dígitos)
        for col in df.columns:
            if "CPF" in col.upper():
                df[col] = df[col].astype(str).str.replace(r"\D", "", regex=True)  # remove tudo que não é dígito

        return df

    except Exception as e:
        raise ValueError(f"Erro ao ler {arquivo} (planilha: {sheet_name}): {e}")
    

def normalizar_colunas(colunas):
    """
    Normaliza nomes de colunas: remove espaços, minúsculas, resolve duplicatas com sufixos.
    
    Exemplo:
        ["  CPF  ", "  CPF  ", "Nome"] → ["cpf_1", "cpf_2", "nome"]
    
    Args:
        colunas (list ou pd.Index): Nomes de colunas
        
    Returns:
        list: Colunas normalizadas sem duplicatas
    """
    from collections import Counter
    
    # Normalizar: strip
    cols = [str(col).strip() for col in colunas]
    
    # Detectar duplicatas
    counts = Counter(cols)
    duplicates = {col for col, count in counts.items() if count > 1}
    
    # Adicionar sufixos para duplicatas
    result = []
    col_count = {}
    
    for col in cols:
        if col in duplicates:
            col_count[col] = col_count.get(col, 0) + 1
            result.append(f"{col}_{col_count[col]}")
        else:
            result.append(col)
    
    return result


def adicionar_coluna_origem(df, nome_arquivo):
    """
    Adiciona coluna com nome do arquivo de origem.
    
    Args:
        df (pd.DataFrame): DataFrame
        nome_arquivo (str): Nome do arquivo
        
    Returns:
        pd.DataFrame: DataFrame com coluna 'arquivo' adicionada
    """
    df["arquivo"] = nome_arquivo
    return df
