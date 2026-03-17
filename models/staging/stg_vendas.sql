/* CAMADA SILVER: TRATAMENTO DE VENDAS
   Objetivo: Padronizar chaves estrangeiras, converter tipos de dados e preparar para JOINs.
*/

-- 1. Criação de uma CTE para isolar a leitura da tabela Bronze de vendas
with raw_vendas as (
    -- O dbt aponta para a tabela definida no seu arquivo sources.yml
    select * from {{ source('ecom_raw', 'vendas') }}
)

-- 2. Bloco principal de transformação
select
    -- Padronização da chave primária: troca 'sal_' (sales) por 'id_'
    replace(trim(id_venda), 'sal_', 'id_') as id_venda,
    
    -- PADRONIZAÇÃO CRÍTICA: As chaves estrangeiras precisam ser idênticas às das tabelas de Clientes e Produtos
    replace(trim(id_cliente), 'cus_', 'id_') as id_cliente,
    replace(trim(id_produto), 'prd_', 'id_') as id_produto,  
    
    -- Tratamento de datas: convertendo para timestamp para permitir análise temporal
    cast(data_venda as timestamp) as data_venda,
    
    
    -- LÓGICA DE GARANTIA: Se tiver 'LOJA' em qualquer lugar, vira 'LOJA_FISICA'
    case 
        when upper(trim(canal_venda)) LIKE '%LOJA%' then 'LOJA_FISICA'
        when upper(trim(canal_venda)) LIKE '%ECOMMERCE%' or upper(trim(canal_venda)) LIKE '%SITE%' then 'ECOMMERCE'
        else 'OUTROS'
    end as canal_venda_nome,
    
    -- Conversão de métricas para tipos numéricos para permitir cálculos matemáticos
    cast(quantidade as int) as quantidade_vendida,
    cast(preco_unitario as decimal(10,2)) as preco_unitario_valor,
    
    -- Adiciona uma coluna de auditoria para controle de processamento
    current_timestamp() as data_processamento_silver

from raw_vendas

-- 3. Filtros de integridade
where 
    id_venda is not null 
    -- Garante que o cabeçalho do CSV não entre como dado
    and id_venda != 'id_venda'