import requests
import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

# ===============================
# 1. CARREGAR TOKEN (.env)
# ===============================
env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(env_path)

TOKEN = os.getenv("API_TRANSPARENCIA_TOKEN")

print("Caminho do .env:", env_path)
print("Existe .env:", env_path.exists())
print("TOKEN carregado:", TOKEN is not None)

# ===============================
# 2. CONFIGURAR DIRETÓRIOS
# ===============================
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ===============================
# 3. HEADERS
# ===============================
headers = {
    "chave-api-dados": TOKEN,
    "User-Agent": "Mozilla/5.0"
}

# ===============================
# 4. FUNÇÃO PRINCIPAL
# ===============================
def coletar_dados():
    url = "https://api.portaldatransparencia.gov.br/api-de-dados/despesas"

    params = {
        "pagina": 1,
        "tamanhoPagina": 5,
        "dataInicial": "01/01/2024",
        "dataFinal": "31/01/2024"
    }

    print("\nTentando API oficial...")

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        print("STATUS:", response.status_code)

        if response.status_code == 200:
            dados = response.json()

            if dados:
                df = pd.DataFrame(dados)

                caminho = RAW_DIR / "despesas_portal.csv"
                df.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")

                print("OK - dados coletados via API:", len(df))
                print("Arquivo salvo em:", caminho)
                return

        print("API bloqueou (403 ou vazio). Usando fallback...")

    except Exception as e:
        print("Erro na API:", e)
        print("Usando fallback...")

    # ===============================
    # 5. FALLBACK (GARANTE PIPELINE)
    # ===============================
    print("\nGerando dados fallback...")

    df = pd.DataFrame({
        "id": range(1, 11),
        "data": pd.date_range(start="2024-01-01", periods=10),
        "valor": [1000, 1200, 980, 1500, 3000, 2000, 1700, 2500, 4000, 3500],
        "fornecedor": ["Empresa A", "Empresa B", "Empresa C", "Empresa A",
                       "Empresa D", "Empresa E", "Empresa B", "Empresa F",
                       "Empresa G", "Empresa H"]
    })

    caminho = RAW_DIR / "fallback_dados.csv"
    df.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")

    print("OK - fallback gerado")
    print("Arquivo salvo em:", caminho)


# ===============================
# 6. EXECUÇÃO
# ===============================
if __name__ == "__main__":
    coletar_dados()