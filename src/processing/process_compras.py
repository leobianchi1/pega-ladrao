import pandas as pd
import os
from sklearn.preprocessing import StandardScaler

# 1. CONFIGURAÇÃO DE CAMINHOS
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Sobe dois níveis (sai de src/processing para a raiz do projeto)
BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))

BRONZE_FILE = os.path.join(BASE_DIR, "data", "bronze", "compras_bronze.csv")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
SILVER_FILE = os.path.join(SILVER_DIR, "compras_silver.csv")

# Garante que a pasta Silver existe na estrutura de diretórios
os.makedirs(SILVER_DIR, exist_ok=True)

# 2. CARREGAR DADOS DA CAMADA BRONZE
if not os.path.exists(BRONZE_FILE):
    print(f"ERRO: Arquivo Bronze não encontrado em {BRONZE_FILE}")
else:
    print(f"Lendo dados da Bronze: {BRONZE_FILE}")
    # Importante: o separador precisa ser o mesmo usado na geração (;)
    df = pd.read_csv(BRONZE_FILE, sep=";")

    # 3. REFINAMENTO E LIMPEZA
    # Focamos apenas em compras que representam gasto real (Aprovado/Pendente)
    # Compras canceladas são ruído para a análise de sobrepreço
    df_silver = df[df["status"] != "cancelado"].copy()

    # 4. INTELIGÊNCIA ESTATÍSTICA (Cálculo de Desvio Interno)
    # Agrupamos por Centro de Custo para entender o comportamento de cada área
    stats = df_silver.groupby("id_centro_custo")["valor"].agg(["mean", "std"]).reset_index()
    stats.columns = ["id_centro_custo", "media_centro", "desvio_centro"]

    # Mesclamos as médias e desvios de volta no DataFrame principal
    df_silver = df_silver.merge(stats, on="id_centro_custo")

    # CÁLCULO DO Z-SCORE (A métrica de alerta)
    # Usamos o .replace(0, 1) no desvio para evitar erro de divisão por zero
    # Isso acontece quando um setor tem apenas uma compra registrada
    df_silver["z_score"] = (
            (df_silver["valor"] - df_silver["media_centro"]) /
            df_silver["desvio_centro"].replace(0, 1)
    ).round(4)

    # 5. NORMALIZAÇÃO (Padrão para modelos de ML)
    # O StandardScaler coloca os valores na mesma escala (média 0, desvio 1)
    scaler = StandardScaler()
    df_silver["valor_normalizado"] = scaler.fit_transform(df_silver[["valor"]]).round(4)

    # 6. SALVAMENTO NA CAMADA SILVER
    # Usamos encoding 'utf-8-sig' para que o Excel abra os acentos corretamente
    df_silver.to_csv(SILVER_FILE, index=False, sep=";", encoding="utf-8-sig")

    print("\n" + "=" * 40)
    print("      CAMADA SILVER FINALIZADA!")
    print(f"Arquivo salvo em: {SILVER_FILE}")
    print("=" * 40)

    # 7. RELATÓRIO DE ALERTAS (Auditoria por Exceção)
    # Filtramos quem está mais de 1.5 desvios acima da média do seu grupo
    print("\n[!] ALERTAS DE RISCO INTERNO (Z-Score > 1.5):")
    alertas = df_silver[df_silver["z_score"] > 1.5]

    if alertas.empty:
        print("Nenhum desvio crítico detectado nesta amostra.")
    else:
        # Ordenamos pelos mais graves para facilitar a auditoria
        colunas_exibicao = ["id_compra", "valor", "id_centro_custo", "z_score"]
        print(alertas[colunas_exibicao].sort_values(by="z_score", ascending=False))