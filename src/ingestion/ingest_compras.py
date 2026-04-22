import pandas as pd
import numpy as np
from datetime import datetime
import os

# 1. CONFIGURAÇÃO DE CAMINHOS
# Descobre onde este script está salvo
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Sobe dois níveis para sair de 'src/ingestion' e chegar na raiz do projeto
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

# Define a pasta de destino e o caminho do arquivo
DATA_PATH = os.path.join(BASE_DIR, "data", "bronze")
FILE_PATH = os.path.join(DATA_PATH, "compras_bronze.csv")

# Cria as pastas necessárias se elas não existirem
os.makedirs(DATA_PATH, exist_ok=True)

# 2. GERAÇÃO DE DADOS SINTÉTICOS
np.random.seed(42)
n = 200

df = pd.DataFrame({
    "id_compra": range(1, n + 1),
    "data_compra": pd.date_range(start="2025-01-01", periods=n, freq="D"),
    "valor": np.random.uniform(100, 10000, n).round(2),
    "id_funcionario": np.random.randint(1, 20, n),
    "id_fornecedor": np.random.randint(1, 10, n),
    "id_centro_custo": np.random.randint(1, 5, n),
    "status": np.random.choice(["aprovado", "pendente", "cancelado"], n)
})

df["data_ingestao"] = datetime.now()

# 3. EXIBIÇÃO DE INFORMAÇÕES NO TERMINAL
print("--- CONFIGURAÇÃO DE DIRETÓRIOS ---")
print(f"RAIZ DO PROJETO: {BASE_DIR}")
print(f"PASTA DE DESTINO: {DATA_PATH}")
print(f"CAMINHO DO ARQUIVO: {FILE_PATH}")
print("-" * 34)

# 4. SALVAMENTO DOS DADOS
# Usamos sep=';' e encoding='utf-8-sig' para abrir direto no Excel sem erros
df.to_csv(FILE_PATH, index=False, sep=";", encoding="utf-8-sig")

print("OK - dados gerados com sucesso!")
print("\nPrimeiras 5 linhas do arquivo:")
print(df.head())