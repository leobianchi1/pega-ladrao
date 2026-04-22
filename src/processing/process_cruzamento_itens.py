import pandas as pd
from pathlib import Path

# ===============================
# 1. CONFIGURAÇÃO DE CAMINHOS
# ===============================
SCRIPT_DIR = Path(__file__).resolve().parent
BASE_DIR = SCRIPT_DIR.parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"
SILVER_DIR = BASE_DIR / "data" / "silver"

CAMINHO_PAG = RAW_DIR / "pagamentos_exemplo.csv"
CAMINHO_ITENS = RAW_DIR / "itens_exemplo.csv"

SILVER_DIR.mkdir(parents=True, exist_ok=True)

# ===============================
# DEBUG DE CAMINHO
# ===============================
print(f"\n📁 RAW_DIR: {RAW_DIR}")
print(f"📄 Procurando: {CAMINHO_PAG.name} e {CAMINHO_ITENS.name}")

# ===============================
# FUNÇÃO
# ===============================
def tratar_moeda(valor):
    if pd.isna(valor):
        return 0.0
    limpo = str(valor).replace('.', '').replace(',', '.')
    try:
        return float(limpo)
    except:
        return 0.0

# ===============================
# 2. VALIDAÇÃO DE ARQUIVOS
# ===============================
if not CAMINHO_PAG.exists():
    print(f"❌ NÃO ENCONTRADO: {CAMINHO_PAG}")

if not CAMINHO_ITENS.exists():
    print(f"❌ NÃO ENCONTRADO: {CAMINHO_ITENS}")

if not CAMINHO_PAG.exists() or not CAMINHO_ITENS.exists():
    print("\n⚠️ COLOQUE OS ARQUIVOS AQUI:")
    print(RAW_DIR)
    exit()

# ===============================
# 3. LEITURA
# ===============================
print("\nLendo ficheiros...")

pag = pd.read_csv(CAMINHO_PAG, sep=";", encoding="utf-8")
itens = pd.read_csv(CAMINHO_ITENS, sep=";", encoding="utf-8")

# ===============================
# 4. LIMPEZA
# ===============================
pag.columns = pag.columns.str.strip().str.lower()
itens.columns = itens.columns.str.strip().str.lower()

pag["id_empenho"] = pag["id_empenho"].astype(str).str.strip()
itens["id_empenho"] = itens["id_empenho"].astype(str).str.strip()

pag["valor_pagamento"] = pag["valor_pagamento"].apply(tratar_moeda)
itens["valor_unitario"] = itens["valor_unitario"].apply(tratar_moeda)

# ===============================
# 5. JOIN
# ===============================
df = pag.merge(itens, on="id_empenho", how="inner")

print(f"\n🔗 Registros após cruzamento: {len(df)}")

if df.empty:
    print("\n⚠️ Nenhum match encontrado.")
    print("Verifique se os IDs são iguais nos dois arquivos.")
    exit()

# ===============================
# 6. Z-SCORE
# ===============================
df["z_score"] = df.groupby("descricao")["valor_unitario"].transform(
    lambda x: (x - x.mean()) / x.std() if len(x) > 1 and x.std() > 0 else 0
)

anomalias = df[df["z_score"] > 1].sort_values(by="z_score", ascending=False)

# ===============================
# 7. OUTPUT
# ===============================
saida = SILVER_DIR / "anomalias_detectadas.csv"
anomalias.to_csv(saida, index=False, sep=";", encoding="utf-8-sig")

print("\n==============================")
print("✅ ANÁLISE FINALIZADA")
print(f"📁 Arquivo salvo: {saida}")
print("==============================")

if not anomalias.empty:
    print("\n🔥 TOP ANOMALIAS:")
    print(anomalias[["id_empenho", "descricao", "valor_unitario", "z_score"]].head(10))
else:
    print("\nNenhuma anomalia relevante encontrada.")