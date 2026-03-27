"""
================================================================================
ORQUESTRADOR DO PIPELINE ETL
================================================================================

Script principal que coordena:
1. Extração de dados CSV
2. Extração de dados XLSX
3. Transformação e mesclagem de colunas
4. Exportação de resultados

Use este arquivo para executar o pipeline completo.
"""

import sys
from pathlib import Path

# Adicionar diretório pai ao path para imports
sys.path.insert(0, str(Path(__file__).parent))

from extract import extract_csv, extract_xlsx
from transform import mesclar_colunas
from pipeline.config import CAMINHO_CSV, CAMINHO_XLSX


def pipeline_csv(salvar=True):
    """
    Executa pipeline de extração e consolidação CSV.
    """
    print("\n" + "█"*80)
    print("█ PIPELINE CSV")
    print("█"*80)
    
    try:
        df_colunas, df_consolidado = extract_csv(CAMINHO_CSV, salvar=salvar)
        print(f"\n✅ Pipeline CSV concluído com sucesso!")
        
        return df_consolidado
    except Exception as e:
        print(f"\n❌ Erro no pipeline CSV: {e}")
        return None


def pipeline_xlsx(salvar=True):
    """
    Executa pipeline de extração e consolidação XLSX.
    """
    print("\n" + "█"*80)
    print("█ PIPELINE XLSX")
    print("█"*80)
    
    try:
        df_consolidado = extract_xlsx(CAMINHO_XLSX, salvar=salvar)
        print(f"\n✅ Pipeline XLSX concluído com sucesso!")
        
        return df_consolidado
    except Exception as e:
        print(f"\n❌ Erro no pipeline XLSX: {e}")
        return None


def pipeline_transformacao(df_consolidado):
    """
    Executa pipeline de transformação e mesclagem.
    """
    print("\n" + "█"*80)
    print("█ PIPELINE DE TRANSFORMAÇÃO")
    print("█"*80)
    
    try:
        df_mesclado = mesclar_colunas(df_consolidado)
        print(f"\n✅ Pipeline de transformação concluído com sucesso!")
        
        return df_mesclado
    except Exception as e:
        print(f"\n❌ Erro no pipeline de transformação: {e}")
        return None


def main(executar_csv=True, executar_xlsx=True, executar_transformacao=False):
    """
    Executa o pipeline completo.
    
    Args:
        executar_csv: Se True, executa pipeline CSV
        executar_xlsx: Se True, executa pipeline XLSX
        executar_transformacao: Se True, executa transformação após consolidação
    """
    print("\n" + "="*80)
    print("🚀 INICIANDO PIPELINE ETL")
    print("="*80)
    
    resultados = {}
    
    # Pipeline CSV
    if executar_csv:
        df_csv = pipeline_csv(salvar=True)
        if df_csv is not None:
            resultados['csv'] = df_csv
    
    # Pipeline XLSX
    if executar_xlsx:
        df_xlsx = pipeline_xlsx(salvar=True)
        if df_xlsx is not None:
            resultados['xlsx'] = df_xlsx
    
    # Transformação
    if executar_transformacao and 'xlsx' in resultados:
        df_mesclado = pipeline_transformacao(resultados['xlsx'])
        if df_mesclado is not None:
            resultados['transformado'] = df_mesclado
    
    # Resumo final
    print("\n" + "="*80)
    print("📊 RESUMO DO PIPELINE")
    print("="*80)
    
    for tipo, df in resultados.items():
        print(f"\n{tipo.upper()}:")
        print(f"  → Linhas: {len(df):,}")
        print(f"  → Colunas: {len(df.columns)}")
        print(f"  → Colunas: {list(df.columns[:5])}...")
    
    if resultados:
        print(f"\n✅ PIPELINE CONCLUÍDO COM SUCESSO!")
        return resultados
    else:
        print(f"\n❌ NENHUM RESULTADO FOI GERADO!")
        return None


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Executor do pipeline ETL"
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        default=True,
        help="Executar pipeline CSV"
    )
    parser.add_argument(
        "--xlsx",
        action="store_true",
        default=True,
        help="Executar pipeline XLSX"
    )
    parser.add_argument(
        "--transformar",
        action="store_true",
        default=False,
        help="Executar transformação após consolidação"
    )
    parser.add_argument(
        "--no-csv",
        action="store_false",
        dest="csv",
        help="NÃO executar pipeline CSV"
    )
    parser.add_argument(
        "--no-xlsx",
        action="store_false",
        dest="xlsx",
        help="NÃO executar pipeline XLSX"
    )
    
    args = parser.parse_args()
    
    resultados = main(
        executar_csv=args.csv,
        executar_xlsx=args.xlsx,
        executar_transformacao=args.transformar
    )
