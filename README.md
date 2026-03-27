# 📊 Pipeline ETL - Consolidação de Dados

## 🎯 Objetivo

Pipeline ETL que automatiza a **extração**, **transformação** e **consolidação** de dados de múltiplos arquivos CSV e XLSX em um único DataFrame estruturado.

---

## 📁 Estrutura

```
projeto-listas-fam/
├── pipeline/
│   ├── extract/      # 📥 Extração (CSV e XLSX)
│   ├── transform/    # 🔄 Transformação e normalização
│   ├── load/         # 💾 Carregamento
│   └── main.py       # 🚀 Orquestrador
├── config.py         # ⚙️ Configurações
├── main.py           # Script principal
└── data/             # Dados processados
```

---

## 🔧 Configuração

Edite `pipeline/config.py` para definir:
- Caminhos de entrada (CSV e XLSX)
- Caminhos de saída (dados consolidados)
- Encodings e separadores

---

## 🚀 Uso

```bash
# Executar tudo (CSV + XLSX)
python pipeline/main.py

# Apenas CSV
python pipeline/main.py --no-xlsx

# Apenas XLSX
python pipeline/main.py --no-csv

# Com transformação/mesclagem
python pipeline/main.py --transformar
```

---

## ✨ Funcionalidades

| Feature | Descrição |
|---------|-----------|
| **Encoding robusto** | Detecta automaticamente UTF-8, Latin-1, CP1252 |
| **Separadores** | Suporta `;`, `,`, `\t` |
| **Normalização** | Padroniza nomes de colunas |
| **Busca fuzzy** | Encontra colunas com variações de nome |
| **Mesclagem** | Combina múltiplas colunas em uma |
| **Rastreabilidade** | Mantém origem de cada registro |

---

## � Requisitos

```bash
pip install pandas openpyxl
```

---

## 📊 Fluxo

```
Extract → Transform → Load
   ↓          ↓         ↓
 CSV/XLSX  Normaliza  Consolidado
```

---

**Versão**: 1.0.0 | **Atualização**: Março 2026
# projeto-listas
