import pandas as pd
from pathlib import Path

# ===============================
# CONFIG
# ===============================
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
SILVER_DIR = BASE_DIR / "data" / "silver"

SILVER_DIR.mkdir(parents=True, exist_ok=True)

arquivo = RAW_DIR / "20251231_Despesas_Pagamento_FavorecidosFinais.csv"

print("Lendo arquivo real...")

# ===============================
# LEITURA
# ===============================
df = pd.read_csv(arquivo, sep=";", encoding="latin1", low_memory=False)

# ===============================
# PADRONIZAÇÃO
# ===============================
df.columns = df.columns.str.lower().str.strip()

col_fornecedor = [c for c in df.columns if "favorecido" in c or "nome" in c][0]
col_valor = [c for c in df.columns if "valor" in c][0]
col_data = [c for c in df.columns if "data" in c][0]

df = df.rename(columns={
    col_fornecedor: "fornecedor",
    col_valor: "valor",
    col_data: "data"
})

# ===============================
# LIMPEZA
# ===============================
df["valor"] = (
    df["valor"]
    .astype(str)
    .str.replace(".", "", regex=False)
    .str.replace(",", ".", regex=False)
)

df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")

df = df.dropna(subset=["valor", "data"])

# ===============================
# FILTRO DEZ/2025
# ===============================
df = df[
    (df["data"].dt.month == 12) &
    (df["data"].dt.year == 2025)
]

print("Registros dezembro 2025:", len(df))

# ===============================
# FILTRO DE QUALIDADE
# ===============================
df_fornec = df.groupby("fornecedor").filter(lambda x: len(x) >= 5)

print("Fornecedores analisados:", df_fornec["fornecedor"].nunique())

# ===============================
# Z-SCORE POR FORNECEDOR
# ===============================
df_fornec["z_score_local"] = df_fornec.groupby("fornecedor")["valor"].transform(
    lambda x: (x - x.mean()) / x.std() if x.std() > 0 else 0
)

# ===============================
# ANOMALIAS
# ===============================
anomalias = df_fornec[df_fornec["z_score_local"] > 2].sort_values(
    by="z_score_local", ascending=False
)

# ===============================
# RANKING
# ===============================
ranking = (
    df_fornec.groupby("fornecedor")
    .agg(
        total=("valor", "sum"),
        media=("valor", "mean"),
        qtd=("valor", "count"),
        risco=("z_score_local", "mean")
    )
    .reset_index()
    .sort_values(by="risco", ascending=False)
)

# ===============================
# SALVAR
# ===============================
ranking.to_csv(SILVER_DIR / "ranking_real_dez_2025.csv", index=False, sep=";")
anomalias.to_csv(SILVER_DIR / "anomalias_reais_dez_2025.csv", index=False, sep=";")

print("\nTOP FORNECEDORES SUSPEITOS:")
print(ranking.head(10))

print("\nTOP ANOMALIAS:")
print(anomalias.head(10))

print("\n===== TOP CONTRATOS =====")

contratos = (
    df_fornec.groupby("código lista")
    .agg(
        total=("valor", "sum"),
        media=("valor", "mean"),
        qtd=("valor", "count")
    )
    .reset_index()
    .sort_values(by="total", ascending=False)
)

print(contratos.head(10))