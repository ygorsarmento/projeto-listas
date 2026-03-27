"""
Script principal para executar o pipeline ETL

Use este arquivo para executar o pipeline completo:
    python main.py
    python main.py --csv
    python main.py --xlsx
    python main.py --transformar
"""

if __name__ == "__main__":
    from pipeline.main import main
    main()
