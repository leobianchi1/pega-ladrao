import requests
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path
import time

# ===============================
# 1. CARREGAR TOKEN
# ===============================
BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

TOKEN = os.getenv("API_TRANSPARENCIA_TOKEN")

print("TOKEN carregado:", TOKEN is not None)

# ===============================
# 2. CONFIGURAÇÃO
# ===============================
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

headers = {
    "chave-api-dados": TOKEN,
    "User-Agent": "Mozilla/5.0"
}

# ===============================
# 3. COLETA (MINISTÉRIO DA SAÚDE)
# ===============================
def coletar_contratos_saude():
    url = "https://api.portaldatransparencia.gov.br/api-de-dados/contratos"

    todos = []

    for pagina in range(1, 11):  # até 10 páginas
        print(f"\nBuscando página {pagina}")

        params = {
            "pagina": pagina,
            "tamanhoPagina": 50,
            "dataInicial": "01/12/2025",
            "dataFinal": "31/12/2025",
            "codigoOrgao": 25000  # Ministério da Saúde
        }

        response = requests.get(url, headers=headers, params=params)

        print("STATUS:", response.status_code)

        if response.status_code == 200:
            dados = response.json()

            if not dados:
                print("Sem mais dados.")
                break

            todos.extend(dados)

        else:
            print("Erro detalhado:", response.text)
            break

        # evitar bloqueio
        time.sleep(1)

    # ===============================
    # 4. SALVAR RESULTADO
    # ===============================
    if todos:
        df = pd.DataFrame(todos)

        caminho = RAW_DIR / "contratos_saude_dez_2025.csv"
        df.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")

        print("\nOK - dados coletados:", len(df))
        print("Arquivo salvo em:", caminho)
    else:
        print("\nNenhum dado coletado.")

# ===============================
# 5. EXECUÇÃO
# ===============================
if __name__ == "__main__":
    coletar_contratos_saude()