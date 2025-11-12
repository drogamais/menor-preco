-- 0) Limpa a tabela de destino
TRUNCATE TABLE silver_menorPreco_notas;

-- ==========================================================
-- NOVO PASSO 0.5: Criar mapa de Geohash -> Localização
-- ==========================================================
DROP TEMPORARY TABLE IF EXISTS temp_mapa_geo;
CREATE TEMPORARY TABLE temp_mapa_geo (
    geohash_prefix VARCHAR(12) PRIMARY KEY, -- Chave é o geohash da cidade
    cidade_normalizada VARCHAR(50),
    microrregiao VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

-- Populamos o mapa a partir da bronze_cidades
-- Usamos GROUP BY para garantir que cada prefixo de geohash seja único
INSERT INTO temp_mapa_geo (geohash_prefix, cidade_normalizada, microrregiao)
SELECT 
    geohash AS geohash_prefix,
    MAX(cidade_normalizada) AS cidade_normalizada,
    MAX(microrregiao) AS microrregiao
FROM bronze_cidades
WHERE geohash IS NOT NULL 
    AND cidade_normalizada IS NOT NULL
    AND microrregiao IS NOT NULL
GROUP BY geohash;

-- ==========================================================
-- PASSO 1: Modificado para usar o mapa GEO
-- ==========================================================
DROP TEMPORARY TABLE IF EXISTS temp_notas_filtradas;
CREATE TEMPORARY TABLE temp_notas_filtradas (
    id_nota VARCHAR(255) NOT NULL,
    `date` DATETIME NULL,
    id_loja VARCHAR(50) NULL,
    geohash VARCHAR(12) NULL,
    gtin VARCHAR(14) NULL,
    descricao TEXT NULL,
    valor DECIMAL(10,2) NULL,
    valor_desconto DECIMAL(10,2) NULL,
    valor_tabela DECIMAL(10,2) NULL,
    cidade VARCHAR(50) NULL,           -- <-- Armazenará a cidade LIMPA
    microrregiao VARCHAR(100) NULL, -- <-- NOVA COLUNA
    data_atualizacao DATE NULL,
    PRIMARY KEY (id_nota),
    INDEX idx_gtin (gtin),
    INDEX idx_id_loja (id_loja),
    INDEX idx_geohash (geohash),
    INDEX idx_cidade (cidade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

INSERT INTO temp_notas_filtradas (id_nota, `date`, id_loja, geohash, gtin, descricao, valor, valor_desconto, valor_tabela, cidade, microrregiao, data_atualizacao)
SELECT 
    n.id_nota, n.`date`, n.id_loja, n.geohash, n.gtin, n.descricao, 
    n.valor, n.valor_desconto, n.valor_tabela, 
    
    -- ==========================================================
    -- CORREÇÃO: Buscamos a cidade e microrregião pelo geohash
    -- ==========================================================
    geo_map.cidade_normalizada AS cidade, 
    geo_map.microrregiao AS microrregiao,
    
    n.data_atualizacao
FROM 
    bronze_menorPreco_notas AS n
INNER JOIN 
    bronze_menorPreco_produtos AS p ON n.gtin = p.gtin
    
-- ==========================================================
-- FAZEMOS O JOIN COM O MAPA GEOHASH
-- Assumindo que o geohash da nota (n.geohash) é mais preciso (mais longo)
-- e o geohash do mapa (geo_map.geohash_prefix) é um prefixo dele.
-- ==========================================================
LEFT JOIN 
    temp_mapa_geo AS geo_map 
    ON LEFT(n.geohash, LENGTH(geo_map.geohash_prefix)) = geo_map.geohash_prefix
    
WHERE 
    n.`date` >= (CURDATE() - INTERVAL 90 DAY);

-- 2) Médias por GTIN e Cidade+GTIN
DROP TEMPORARY TABLE IF EXISTS temp_media_gtin;
CREATE TEMPORARY TABLE temp_media_gtin (
    gtin VARCHAR(14) PRIMARY KEY,
    media_gtin DECIMAL(10,4) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

INSERT INTO temp_media_gtin (gtin, media_gtin)
SELECT gtin, AVG(valor)
FROM temp_notas_filtradas
GROUP BY gtin;

DROP TEMPORARY TABLE IF EXISTS temp_media_cidade_gtin;
CREATE TEMPORARY TABLE temp_media_cidade_gtin (
    gtin VARCHAR(14),
    cidade VARCHAR(50),
    media_cidade_gtin DECIMAL(10,4) NULL,
    PRIMARY KEY (gtin, cidade)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

INSERT INTO temp_media_cidade_gtin (gtin, cidade, media_cidade_gtin)
SELECT gtin, cidade, AVG(valor)
FROM temp_notas_filtradas
GROUP BY gtin, cidade;

-- 3) Produtos únicos (agrupando por GTIN)
DROP TEMPORARY TABLE IF EXISTS temp_produtos_unicos;
CREATE TEMPORARY TABLE temp_produtos_unicos (
    gtin VARCHAR(14) NOT NULL,
    id_produto VARCHAR(20) NULL,
    PRODUTO TEXT NULL,
    PRIMARY KEY (gtin)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

INSERT INTO temp_produtos_unicos (gtin, id_produto, PRODUTO)
SELECT
    p.gtin AS gtin,
    MAX(p.id_produto) AS id_produto, -- <<< CORREÇÃO AQUI
    MAX(TRIM(
        CONCAT_WS(' - ',
            p.gtin,
            NULLIF(p.descricao, ''),
            NULLIF(p.fabricante, '')
        )
    )) AS PRODUTO
FROM bronze_menorPreco_produtos p
INNER JOIN (
    SELECT DISTINCT gtin 
    FROM temp_notas_filtradas
    WHERE gtin IS NOT NULL
) AS gtin_filtrados ON p.gtin = gtin_filtrados.gtin
GROUP BY p.gtin;

-- 3c) Calcular Ranks de Bandeira (por Cidade e Microrregião)
DROP TEMPORARY TABLE IF EXISTS temp_ranks;
CREATE TEMPORARY TABLE temp_ranks (
    cidade VARCHAR(50),
    microrregiao VARCHAR(100),
    bandeira VARCHAR(100),
    rank_cidade INT NULL,
    rank_microrregiao INT NULL,
    PRIMARY KEY (cidade, microrregiao, bandeira),
    INDEX idx_cidade_bandeira (cidade, bandeira),
    INDEX idx_micro_bandeira (microrregiao, bandeira)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
    
    INSERT INTO temp_ranks (cidade, microrregiao, bandeira, rank_cidade, rank_microrregiao)
WITH
base_contagem AS (
    SELECT 
        n.descricao,
        n.cidade,        -- <-- Já está normalizada!
        n.microrregiao,  -- <-- Já temos aqui!
        l.bandeira
    FROM temp_notas_filtradas AS n
    LEFT JOIN bronze_menorPreco_lojas AS l 
        ON n.id_loja = l.id_loja
    -- NÃO PRECISAMOS MAIS DO JOIN COM temp_mapa_cidade_micro
    WHERE l.bandeira IS NOT NULL
        AND l.bandeira NOT IN ('MERCADO', 'OUTRAS BANDEIRAS', 'DROGAMAIS')
),
contagens AS (
    SELECT
        cidade, microrregiao, bandeira,
        COUNT(descricao) AS contagem
    FROM base_contagem
    GROUP BY cidade, microrregiao, bandeira
)
SELECT
    cidade, microrregiao, bandeira,
    DENSE_RANK() OVER (
        PARTITION BY cidade 
        ORDER BY contagem DESC
    ) AS rank_cidade,
    DENSE_RANK() OVER (
        PARTITION BY microrregiao 
        ORDER BY contagem DESC
    ) AS rank_microrregiao
FROM contagens;

-- 4) INSERT final (join com todas as dimensões)
INSERT INTO silver_menorPreco_notas (
    id_nota,
    `date`,
    id_loja,
    geohash,
    id_produto,
    gtin,
    descricao,
    valor,
    valor_desconto,
    valor_tabela,
    cidade,
    data_atualizacao,
    bandeira,
    latitude,
    longitude,
    nome_fantasia,
    razao_social,
    PRODUTO,
    microrregiao,
    DateRelacionamento,
    media_gtin,
    media_cidade_gtin,
    rank_cidade,
    rank_microrregiao,
    considerado
)
SELECT
    n.id_nota,
    n.`date`,
    n.id_loja,
    n.geohash,
    p.id_produto,
    n.gtin,
    n.descricao,
    n.valor,
    n.valor_desconto,
    n.valor_tabela,
    n.cidade,
    n.data_atualizacao,
    l.bandeira,
    l.latitude,
    l.longitude,
    COALESCE(l.nome_fantasia, 'NÃO ENCONTRADO'),
    l.razao_social,
    p.PRODUTO,
    n.microrregiao,
    CAST(n.`date` AS DATE),
    m_gtin.media_gtin,
    m_cid_gtin.media_cidade_gtin,
    r.rank_cidade,
    r.rank_microrregiao,
    CASE
    WHEN m_gtin.media_gtin IS NULL THEN NULL
    WHEN n.valor >= (m_gtin.media_gtin * 1.5)
        OR n.valor <= (m_gtin.media_gtin * 0.5)
        THEN 'N'
    ELSE 'S'
END AS considerado
FROM temp_notas_filtradas AS n

LEFT JOIN bronze_menorPreco_lojas AS l
    ON n.id_loja = l.id_loja
    
LEFT JOIN temp_produtos_unicos AS p
    ON n.gtin = p.gtin
    
LEFT JOIN temp_media_gtin AS m_gtin
    ON n.gtin = m_gtin.gtin
    
LEFT JOIN temp_media_cidade_gtin AS m_cid_gtin
    ON n.gtin = m_cid_gtin.gtin
    AND n.cidade = m_cid_gtin.cidade
    
LEFT JOIN temp_ranks AS r
    ON n.cidade <=> r.cidade 
    AND n.microrregiao <=> r.microrregiao
    AND l.bandeira <=> r.bandeira;

-- 5) Limpeza final
DROP TEMPORARY TABLE IF EXISTS temp_notas_filtradas;
DROP TEMPORARY TABLE IF EXISTS temp_media_gtin;
DROP TEMPORARY TABLE IF EXISTS temp_media_cidade_gtin;
DROP TEMPORARY TABLE IF EXISTS temp_produtos_unicos;
DROP TEMPORARY TABLE IF EXISTS temp_mapa_cidade_micro;
DROP TEMPORARY TABLE IF EXISTS temp_cidades_unicas;
DROP TEMPORARY TABLE IF EXISTS temp_mapa_geo;
DROP TEMPORARY TABLE IF EXISTS temp_ranks;
