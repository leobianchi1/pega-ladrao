import pandas as pd
from pathlib import Path
import re


# ===============================
# 1. FUNÇÕES DE LIMPEZA PESADA
# ===============================
def limpar_geral(texto):
    """Extrai apenas números e remove zeros à esquerda."""
    if pd.isna(texto): return ""
    nums = re.sub(r'\D', '', str(texto))
    return nums.lstrip('0')


# ===============================
# 2. CONFIGURAÇÕES
# ===============================
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
SILVER_DIR = BASE_DIR / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)

print("Iniciando Camada Silver com Multicruzamento...")

# ===============================
# 3. CARGA DOS DADOS
# ===============================
pag = pd.read_csv(RAW_DIR / "20251231_Despesas_Pagamento.csv", sep=";", encoding="latin1", low_memory=False)
itens = pd.read_csv(RAW_DIR / "20251231_Despesas_ItemEmpenho.csv", sep=";", encoding="latin1", low_memory=False)

# Padroniza tudo para minúsculo
pag.columns = pag.columns.str.lower().str.strip()
itens.columns = itens.columns.str.lower().str.strip()

# ===============================
# 4. PREPARAÇÃO DOS IDS
# ===============================
print("Preparando chaves de ligação...")

# Nos ITENS, o ID é o 'id empenho'
itens['lista'] = itens['id empenho'].apply(limpar_geral)

# Nos PAGAMENTOS, vamos criar três tentativas de 'lista'
# Tentativa 1: O código resumido (muitas vezes contém o número do empenho)
pag['lista_resumida'] = pag['código pagamento resumido'].apply(limpar_geral)

# Tentativa 2: O número do processo
pag['lista_processo'] = pag['processo'].apply(limpar_geral)

# Tentativa 3: O ID que está dentro da Observação (que você já tentou)
pag['lista_obs'] = pag['observação'].str.extract(r'(\d{7,10})')[0].fillna('').apply(limpar_geral)

# ===============================
# 5. O MULTICRUZAMENTO
# ===============================
print("Executando tentativas de cruzamento...")

# Tentativa A: Pelo Código Resumido
df = pag.merge(itens, left_on='lista_resumida', right_on='lista', how='inner')

# Tentativa B: Se a A falhou, tenta pelo Processo
if len(df) == 0:
    print("Tentativa A falhou. Tentando pelo Número do Processo...")
    df = pag.merge(itens, left_on='lista_processo', right_on='lista', how='inner')

# Tentativa C: Se a B falhou, tenta pela Observação
if len(df) == 0:
    print("Tentativa B falhou. Tentando pela Observação...")
    df = pag.merge(itens, left_on='lista_obs', right_on='lista', how='inner')

# ===============================
# 6. RESULTADOS E SALVAMENTO
# ===============================
print(f"\nRegistros cruzados com sucesso: {len(df)}")

if len(df) > 0:
    # Mapeia colunas para o resto do sistema
    df = df.rename(columns={
        'descrição': 'descricao',
        'valor unitário': 'valor_item',
        'valor do pagamento convertido pra r$': 'valor_pago'
    })

    # Cálculos de Auditoria
    df["preco_unitario"] = pd.to_numeric(df["valor_item"], errors='coerce') / pd.to_numeric(df["quantidade"],
                                                                                            errors='coerce')
    df["z_score"] = df.groupby("descricao")["preco_unitario"].transform(
        lambda x: (x - x.mean()) / x.std() if len(x) > 1 and x.std() > 0 else 0
    )

    output_path = SILVER_DIR / "anomalias_preco_unitario.csv"
    df[df["z_score"] > 2].to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")
    print(f"Sucesso! Arquivo gerado em: {output_path}")
else:
    print("\n[!] CRÍTICO: Nenhuma das 3 tentativas de cruzamento funcionou.")
    print("Isso indica que os arquivos não são do mesmo conjunto de dados ou não possuem relação direta.")