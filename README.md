Análise de despesas públicas com foco em variação de preços

Este projeto nasceu de uma inquietação simples.
Será que dá para identificar padrões estranhos em compras públicas usando apenas dados abertos e uma lógica estatística básica?

A resposta é sim.

O que foi feito

A ideia foi montar um fluxo simples, mas consistente:

ler dados de pagamentos
ler dados de itens comprados
cruzar essas informações
calcular o preço unitário
comparar itens iguais entre si

A partir disso, usei Z-score para identificar valores que fogem do padrão.

Nada sofisticado demais. Mas suficiente para começar a enxergar inconsistências.

O que o projeto mostra

Quando você compara o mesmo item, aparecem situações interessantes.

Exemplo:

luva hospitalar com preço médio próximo de 2,50
registros do mesmo item chegando a 6,80

Ou ainda:

seringa descartável na faixa de 1,20
registros acima de 4,90

Isso não prova irregularidade.
Mas levanta uma pergunta legítima.

Por que o mesmo item varia tanto?

Como funciona a detecção

Para cada item, é calculado o desvio em relação à média.

Se um valor se distancia demais do comportamento normal daquele item, ele é marcado como anomalia.

Na prática:

valores próximos da média passam despercebidos
valores muito acima começam a aparecer no radar
Estrutura do projeto

O projeto foi organizado de forma simples:

pasta de coleta
pasta de ingestão
pasta de processamento
dados separados entre bruto e tratado

Isso facilita evoluir depois sem bagunçar tudo.

O que dá para evoluir

Esse é só o começo.

Daria para avançar bastante:

usar dados reais do Portal da Transparência
cruzar com contratos e fornecedores
analisar por órgão público
criar um painel visual
construir um score de risco mais completo
Por que esse projeto importa

Dados públicos estão aí, mas raramente são explorados de forma prática.
