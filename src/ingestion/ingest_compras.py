import pandas as pd
import numpy as np
from datetime import datetime
import os

# Criar pasta data se não existir
os.makedirs("data", exist_ok=True)

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

# Salvar
df.to_csv(""data/bronze/compras_raw.csv", index=False, sep=";", encoding="utf-8-sig")

print("OK - dados gerados")
print(df.head())
