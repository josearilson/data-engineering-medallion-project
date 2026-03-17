/* CAMADA SILVER: TRATAMENTO DE CLIENTES
   Objetivo: Limpar prefixos, remover títulos de nomes e padronizar localizações.
*/

-- 1. Criação de uma CTE para isolar a leitura da tabela Bronze (Raw)
with raw_clientes as (
    -- IMPORTANTE: Verifique se os nomes 'ecom_raw' e 'clientes' estão entre aspas simples
    select * from {{ source('ecom_raw', 'clientes') }}
)

-- 2. Bloco principal de transformação e limpeza
select
    -- Remove espaços em branco e substitui o prefixo 'cus_' por 'id_' para padronizar chaves
    replace(trim(id_cliente), 'cus_', 'id_') as id_cliente,
    
    -- Usa Expressão Regular (Regex) para remover títulos (Srta., Dra., etc.) do nome
    -- O trim remove espaços que sobrariam após a remoção do título
    trim(regexp_replace(nome_cliente, 'Sra\\.|Srta\\.|Dra\\.|Dr\\.|Sr\\.', '')) as nome_cliente,
     
    -- Transforma o estado em letras MAIÚSCULAS e remove espaços para evitar duplicidade em filtros
    upper(trim(estado)) as estado_codigo,
    
    -- Padroniza o nome do país também em MAIÚSCULAS
    upper(trim(pais)) as pais_nome,
    
    -- Garante que a coluna de data seja interpretada como Timestamp pelo Databricks
    cast(data_cadastro as timestamp) as data_cadastro,
    
    -- Adiciona uma coluna de 'Time-Travel' para saber exatamente quando o dado foi processado na Silver
    current_timestamp() as data_processamento_silver

from raw_clientes

where 
    -- Descarta IDs nulos
    id_cliente is not null 
    
    -- Evita processar a linha de cabeçalho do CSV
    and id_cliente != 'id_cliente'


    