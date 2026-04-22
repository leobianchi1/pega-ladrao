# 🕵️ pega-ladrao

Sistema de detecção de possíveis sobrepreços em compras corporativas utilizando análise estatística e comparação com mercado externo.

---

## 🎯 Problema

Empresas e instituições frequentemente realizam compras com pouca visibilidade sobre o preço real de mercado, abrindo espaço para:

- sobrepreço
- inconsistências entre setores
- falhas de controle

---

## 🧠 Solução

Pipeline de dados em 3 camadas:

### 🥉 Bronze
- Geração e ingestão de dados brutos

### 🥈 Silver
- Limpeza e padronização
- Cálculo de Z-Score por centro de custo
- Identificação de desvios internos

### 🥇 Gold
- Integração com preços de mercado (API)
- Ajuste por deflação
- Comparação com referências públicas (TCU/TCE)
- Classificação de risco

---

## ⚙️ Tecnologias

- Python
- Pandas
- Scikit-learn
- Requests
- Git / GitHub

---

## 📊 Resultado

O sistema identifica automaticamente compras com potencial de:

- sobrepreço confirmado
- desvio de mercado
- necessidade de investigação

Exemplo:

| id_compra | valor | preco_deflacionado | analise_final |
|----------|------|--------------------|--------------|
| 70       | 9870 | 5200               | CRÍTICO      |

---

## 🚀 Como rodar

```bash
python src/ingestion/ingest_compras.py
python src/enrichment/enrich_compras.py
