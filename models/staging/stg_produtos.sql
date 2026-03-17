/* CAMADA SILVER: TRATAMENTO DE PRODUTOS
   Objetivo: Padronizar IDs, converter preços para decimal e limpar metadados de produtos.
*/

-- 1. Criação de uma CTE para isolar a leitura da tabela Bronze de produtos
with raw_produtos as (
    -- O dbt aponta para a tabela definida no seu arquivo sources.yml
    select * from {{ source('ecom_raw', 'produtos') }}
)

-- 2. Bloco principal de transformação
select
    -- Padronização de chave: troca o prefixo 'prd_' por 'id_'
    replace(trim(id_produto), 'prd_', 'id_') as id_produto,
    
    -- Limpeza simples de espaços no nome do produto
    trim(nome_produto) as nome_produto,
    
    -- Padronização de categorias e marcas em MAIÚSCULAS para evitar duplicidade em relatórios
    upper(trim(categoria)) as categoria_nome,
    upper(trim(marca)) as marca_nome,
    
    -- CONVERSÃO CRÍTICA: Transforma o preço (que vem como texto) em Decimal (10,2)
    -- Isso permite somar valores e calcular médias na camada Gold
    cast(preco_atual as decimal(10,2)) as preco_valor,
    
    -- Garante o formato de data e hora para a criação do registro
    cast(data_criacao as timestamp) as data_cadastro,
    
    -- Coluna de auditoria para controle de processamento
    current_timestamp() as data_processamento_silver

from raw_produtos

-- 3. Filtros de integridade
where 
    id_produto is not null 
    -- Garante que o cabeçalho do CSV não entre como dado
    and id_produto != 'id_produto'