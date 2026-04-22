import pandas as pd
from pathlib import Path

# ===============================
# 1. CONFIGURAÇÃO
# ===============================
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
BRONZE_DIR = BASE_DIR / "data" / "bronze"

BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# ===============================
# 2. MAPA DE COLUNAS
# ===============================
MAPA_COLUNAS = {
    # valor
    "valor_total": "valor",
    "vlr": "valor",
    "preco": "valor",
    "valor": "valor",

    # data
    "data_compra": "data",
    "dt_empenho": "data",
    "data": "data",

    # fornecedor
    "fornecedor_nome": "fornecedor",
    "razao_social": "fornecedor",
    "favorecido": "fornecedor",
    "fornecedor": "fornecedor"
}

# ===============================
# 3. PADRONIZAÇÃO
# ===============================
def padronizar_colunas(df):
    df.columns = df.columns.str.lower().str.strip()

    rename_dict = {}
    for col in df.columns:
        if col in MAPA_COLUNAS:
            rename_dict[col] = MAPA_COLUNAS[col]

    return df.rename(columns=rename_dict)

# ===============================
# 4. GARANTIR FORNECEDOR
# ===============================
def garantir_fornecedor(df):
    if "fornecedor" in df.columns:
        df["fornecedor"] = df["fornecedor"].fillna("desconhecido")
        return df

    if "id_fornecedor" in df.columns:
        df["fornecedor"] = "Fornecedor_" + df["id_fornecedor"].astype(str)
    else:
        df["fornecedor"] = "desconhecido"

    return df

# ===============================
# 5. LIMPEZA
# ===============================
def limpar_dados(df):
    # valor
    if "valor" in df.columns:
        df["valor"] = (
            df["valor"]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        )
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    # data
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], errors="coerce")

    # fornecedor
    df["fornecedor"] = df["fornecedor"].astype(str).str.strip()

    return df

# ===============================
# 6. PROCESSAMENTO RAW → BRONZE
# ===============================
def processar_raw():
    arquivos = list(RAW_DIR.glob("*.csv"))

    if not arquivos:
        print("Nenhum arquivo RAW encontrado.")
        return

    print(f"Encontrados {len(arquivos)} arquivos RAW")

    for arquivo in arquivos:
        print(f"\nProcessando: {arquivo.name}")

        try:
            df = pd.read_csv(arquivo, sep=";", encoding="utf-8", low_memory=False)

            # 1. padronizar
            df = padronizar_colunas(df)

            # 2. garantir fornecedor
            df = garantir_fornecedor(df)

            # 3. limpar
            df = limpar_dados(df)

            # 4. evitar duplicação no nome
            nome_base = arquivo.stem.replace("_bronze", "")
            caminho_saida = BRONZE_DIR / (nome_base + "_bronze.csv")

            df.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8-sig")

            print(f"OK -> salvo em: {caminho_saida}")

        except Exception as e:
            print(f"Erro ao processar {arquivo.name}: {e}")

# ===============================
# 7. EXECUÇÃO
# ===============================
if __name__ == "__main__":
    processar_raw()