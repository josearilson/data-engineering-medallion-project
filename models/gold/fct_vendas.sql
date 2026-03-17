{{ config(materialized='table') }}

/* CAMADA GOLD: FATO VENDAS (TABELA UNIFICADA)
   Objetivo: Disponibilizar uma tabela completa para a equipe de Power BI,
   unindo transações, dados de clientes e detalhes de produtos com métricas temporais.
*/

-- 1. Importação das tabelas da Camada Silver
with clientes as (
    select * from {{ ref('stg_clientes') }}
),

produtos as (
    select * from {{ ref('stg_produtos') }}
),

vendas as (
    select * from {{ ref('stg_vendas') }}
)

-- 2. Join das tabelas e transformações para criar a tabela de fato
select
    -- Identificadores
    v.id_venda,
    v.id_cliente,
    v.id_produto,
    v.data_venda,

    -- Transformações Temporais 
    year(v.data_venda) as ano,
    date_format(v.data_venda, 'yyyy-MM') as mes_ano,

    -- Atributos Descritivos (Dimensões)
    c.nome_cliente,
    c.estado_codigo as estado,
    c.pais_nome as pais,
    p.nome_produto,
    p.categoria_nome as categoria,
    p.marca_nome as marca,
    v.canal_venda_nome as canal_venda,

    -- Métricas Quantitativas
    v.quantidade_vendida as quantidade,
    v.preco_unitario_valor as preco_unitario,
    
    -- Cálculo de Faturamento (Equivalente ao round(col("quantidade") * col("preco_unitario"), 2))
    cast(round(v.quantidade_vendida * v.preco_unitario_valor, 2) as decimal(10,2)) as total_venda,

    -- Auditoria
    current_timestamp() as data_processamento_gold

from vendas v
-- INNER JOIN garante que só teremos vendas com clientes e produtos válidos  
inner join clientes c on v.id_cliente = c.id_cliente
inner join produtos p on v.id_produto = p.id_produto

where v.id_venda is not null