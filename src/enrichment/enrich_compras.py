import pandas as pd
import os
import requests

# =================================================================
# 1. CONFIGURAÇÃO DE CAMINHOS E DIRETÓRIOS
# =================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

SILVER_FILE = os.path.join(BASE_DIR, "data", "silver", "compras_silver.csv")
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
GOLD_FILE = os.path.join(GOLD_DIR, "compras_gold.csv")

os.makedirs(GOLD_DIR, exist_ok=True)


# =================================================================
# 2. FUNÇÕES DE ENRIQUECIMENTO (INTELIGÊNCIA DO MOTOR)
# =================================================================

def buscar_preco_mercado(produto="notebook"):
    """Consulta preços atuais no Mercado Livre com proteção contra bloqueios."""
    url = f"https://api.mercadolibre.com/sites/MLB/search?q={produto}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Aviso API: Status {response.status_code}. Usando fallback.")
            return None

        data = response.json()
        if "results" in data and len(data["results"]) > 0:
            precos = [item["price"] for item in data["results"][:5] if "price" in item]
            return sum(precos) / len(precos)
        return None
    except Exception as e:
        print(f"Erro de conexão: {e}")
        return None


def calcular_deflacao(preco_atual, meses_atras=6, taxa=0.005):
    """Ajusta o preço de hoje para o valor justo na data da compra."""
    fator = (1 + taxa) ** meses_atras
    return round(preco_atual / fator, 2)


def consultar_referencia_governo(produto="notebook"):
    """Simula consulta a preços homologados pelo TCE/TCU/Senac."""
    referencias = {
        "notebook": 4850.00,
        "cadeira": 950.00,
        "monitor": 1250.00
    }
    return referencias.get(produto.lower(), 1100.00)


# =================================================================
# 3. PROCESSAMENTO DA CAMADA GOLD
# =================================================================

if not os.path.exists(SILVER_FILE):
    print(f"ERRO: Arquivo Silver não encontrado em {SILVER_FILE}")
else:
    print(f"Lendo dados refinados da Silver: {SILVER_FILE}")
    df = pd.read_csv(SILVER_FILE, sep=";")

    # 1. OBTER PREÇO DE MERCADO ATUAL
    print("Buscando referência externa e aplicando deflação...")
    preco_hoje = buscar_preco_mercado("notebook")

    # Fallback Inteligente: Se a API falhar, usa a média da própria empresa como base
    if preco_hoje is None:
        preco_hoje = df["valor"].mean()

    # 2. ENRIQUECIMENTO DE COLUNAS
    df["preco_mercado_hoje"] = preco_hoje

    # Aplicando sua função de deflação (supondo compras de 6 meses atrás)
    df["preco_deflacionado"] = df["preco_mercado_hoje"].apply(lambda x: calcular_deflacao(x, 6))

    # Cruzamento com órgãos públicos
    df["preco_licitacao_tcu"] = consultar_referencia_governo("notebook")

    # Cálculo do gap financeiro (Valor Pago vs Preço que deveria ser na época)
    df["diferenca_real"] = (df["valor"] - df["preco_deflacionado"]).round(2)


    # 3. CLASSIFICAÇÃO DE RISCO (TRIANGULAÇÃO)
    def definir_veredito_final(row):
        # Regra de Ouro: Z-Score alto + Valor > Deflacionado + Valor > TCU
        if row["z_score"] > 1.5 and row["valor"] > row["preco_deflacionado"]:
            if row["valor"] > row["preco_licitacao_tcu"]:
                return "CRÍTICO: Sobrepreço Confirmado"
            return "ALTO: Desvio de Mercado"
        elif row["z_score"] > 1.2 or row["diferenca_real"] > 1000:
            return "MEDIO: Investigar Detalhes"
        else:
            return "NORMAL"


    df["analise_final"] = df.apply(definir_veredito_final, axis=1)

    # 4. SALVAMENTO FINAL
    df.to_csv(GOLD_FILE, index=False, sep=";", encoding="utf-8-sig")

    print("\n" + "=" * 45)
    print("      CAMADA GOLD FINALIZADA!")
    print(f"Arquivo gerado em: {GOLD_FILE}")
    print("=" * 45)

    # 5. RESUMO EXECUTIVO (O que o Diretor quer ver)
    total_sobrepreco = df[df["analise_final"].str.contains("CRÍTICO|ALTO")]["diferenca_real"].sum()

    print(f"\n[!] RESULTADO DA AUDITORIA:")
    print(f"POTENCIAL DE RECUPERAÇÃO: R$ {total_sobrepreco:,.2f}")

    print("\nTOP SUSPEITOS IDENTIFICADOS:")
    colunas_finais = ["id_compra", "valor", "preco_deflacionado", "preco_licitacao_tcu", "analise_final"]
    print(df.sort_values(by="diferenca_real", ascending=False)[colunas_finais].head(10))