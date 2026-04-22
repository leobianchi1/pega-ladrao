import pandas as pd
from pathlib import Path
from sklearn.preprocessing import StandardScaler

# ===============================
# 1. CONFIGURAÇÃO
# ===============================
BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

SILVER_DIR.mkdir(parents=True, exist_ok=True)

# ===============================
# 2. CARREGAR DADOS (SEM DUPLICAÇÃO)
# ===============================
def carregar_bronze():
    arquivos = [
        f for f in BRONZE_DIR.glob("*_bronze.csv")
        if "bronze_bronze" not in f.name
    ]

    if not arquivos:
        print("Nenhum arquivo bronze encontrado.")
        return pd.DataFrame()

    dfs = []

    for arquivo in arquivos:
        print(f"Lendo: {arquivo.name}")
        df = pd.read_csv(arquivo, sep=";")
        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)
    print(f"Total de registros: {len(df_final)}")

    return df_final

# ===============================
# 3. GARANTIR FORNECEDOR
# ===============================
def garantir_fornecedor(df):
    if "fornecedor" not in df.columns:
        df["fornecedor"] = "desconhecido"

    df["fornecedor"] = df["fornecedor"].fillna("desconhecido")
    return df

# ===============================
# 4. PROCESSAMENTO
# ===============================
def processar_silver(df):
    if df.empty:
        print("DataFrame vazio.")
        return df

    df = garantir_fornecedor(df)

    # remover valores inválidos
    df = df.dropna(subset=["valor"])

    # ===========================
    # MÉTRICAS POR FORNECEDOR
    # ===========================
    media = df.groupby("fornecedor")["valor"].mean().reset_index()
    media.columns = ["fornecedor", "media_fornecedor"]

    df = df.merge(media, on="fornecedor", how="left")

    # ===========================
    # Z-SCORE GLOBAL
    # ===========================
    df["z_score_global"] = (
        (df["valor"] - df["valor"].mean()) / df["valor"].std()
    )

    # ===========================
    # NORMALIZAÇÃO
    # ===========================
    scaler = StandardScaler()
    df["valor_normalizado"] = scaler.fit_transform(df[["valor"]])

    return df

# ===============================
# 5. SALVAR RESULTADO
# ===============================
def salvar(df):
    caminho = SILVER_DIR / "compras_silver_real.csv"
    df.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")

    print("\nArquivo salvo em:", caminho)

# ===============================
# 6. RANKING DE FORNECEDORES
# ===============================
def gerar_ranking(df):
    print("\n===== RANKING DE FORNECEDORES =====")

    ranking = (
        df.groupby("fornecedor")
        .agg(
            total_gasto=("valor", "sum"),
            media_valor=("valor", "mean"),
            qtd_compras=("valor", "count"),
            risco_medio=("z_score_global", "mean")
        )
        .reset_index()
    )

    ranking = ranking.sort_values(by="risco_medio", ascending=False)

    print(ranking.head(10))

    caminho = SILVER_DIR / "ranking_fornecedores.csv"
    ranking.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")

    print("\nRanking salvo em:", caminho)

# ===============================
# 7. EXECUÇÃO
# ===============================
if __name__ == "__main__":
    print("Iniciando processamento SILVER...")

    df = carregar_bronze()

    if not df.empty:
        df = processar_silver(df)
        salvar(df)
        gerar_ranking(df)
    else:
        print("Nada para processar.")