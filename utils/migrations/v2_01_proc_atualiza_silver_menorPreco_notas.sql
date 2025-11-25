-- Definição da procedure: proc_atualiza_silver_menorPreco_notas

DELIMITER $$

CREATE PROCEDURE `proc_atualiza_silver_menorPreco_notas`()
BEGIN
    -- Variáveis de Controle
    DECLARE v_watermark DATETIME;
    DECLARE v_qtd_antes INT DEFAULT 0;
    DECLARE v_qtd_depois INT DEFAULT 0;
    DECLARE v_total_touched INT DEFAULT 0;
    DECLARE v_novos INT DEFAULT 0;
    DECLARE v_alterados INT DEFAULT 0;

    -- 1. Pega o Watermark
    SELECT MAX(data_atualizacao) INTO v_watermark FROM silver_menorPreco_notas;
    
	 IF v_watermark IS NULL THEN 
	 	SET v_watermark = '2000-01-01 00:00:00'; 
	 END IF;

    -- ==========================================================
    -- AUDITORIA: Contagem Inicial
    -- ==========================================================
    SELECT COUNT(*) INTO v_qtd_antes FROM silver_menorPreco_notas;

    -- ==========================================================
    -- PASSO 0.5: Mapas e Tops
    -- ==========================================================
    DROP TEMPORARY TABLE IF EXISTS temp_mapa_geo;
    CREATE TEMPORARY TABLE temp_mapa_geo (
        geohash_prefix VARCHAR(12) PRIMARY KEY, 
        cidade_normalizada VARCHAR(50),
        microrregiao VARCHAR(100)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;

    INSERT INTO temp_mapa_geo (geohash_prefix, cidade_normalizada, microrregiao)
    SELECT geohash, MAX(cidade_normalizada), MAX(microrregiao)
    FROM bronze_cidades
    WHERE geohash IS NOT NULL AND cidade_normalizada IS NOT NULL
    GROUP BY geohash;

    DROP TEMPORARY TABLE IF EXISTS temp_top_gtins;
    CREATE TEMPORARY TABLE temp_top_gtins (gtin VARCHAR(14) PRIMARY KEY) ENGINE=InnoDB;

    INSERT INTO temp_top_gtins (gtin)
    SELECT n.gtin FROM bronze_menorPreco_notas n
    WHERE n.`date` >= (CURDATE() - INTERVAL 360 DAY)
    GROUP BY n.gtin ORDER BY COUNT(*) DESC;

    -- ==========================================================
    -- PASSO 1: Carga dos dados
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
        cidade VARCHAR(50) NULL,            
        microrregiao VARCHAR(100) NULL,
        data_atualizacao DATETIME NULL,
        PRIMARY KEY (id_nota),
        INDEX idx_gtin (gtin),
        INDEX idx_id_loja (id_loja),
        INDEX idx_geohash (geohash),
        INDEX idx_cidade (cidade)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
    
    INSERT INTO temp_notas_filtradas (
        id_nota, `date`, id_loja, geohash, gtin, descricao, valor, 
        valor_desconto, valor_tabela, cidade, microrregiao, data_atualizacao
    )

    SELECT 
        n.id_nota, n.`date`, n.id_loja, n.geohash, n.gtin, n.descricao, 
        n.valor, n.valor_desconto, n.valor_tabela, 
        geo_map.cidade_normalizada AS cidade, 
        geo_map.microrregiao AS microrregiao,
        n.data_atualizacao 
    FROM 
        bronze_menorPreco_notas AS n
    INNER JOIN temp_top_gtins AS top ON n.gtin = top.gtin
    LEFT JOIN temp_mapa_geo AS geo_map 
        ON n.geohash = geo_map.geohash_prefix
    WHERE 
        n.data_atualizacao > v_watermark;
    
    -- ==========================================================
    -- Cálculos Intermediários
    -- ==========================================================
    DROP TEMPORARY TABLE IF EXISTS temp_media_gtin;
    CREATE TEMPORARY TABLE temp_media_gtin (
	 	 gtin VARCHAR(14) PRIMARY KEY, 
		 media_gtin DECIMAL(10,4) NULL) 
	 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
		 
    INSERT INTO temp_media_gtin (gtin, media_gtin) 
	 SELECT gtin, AVG(valor) 
	 FROM temp_notas_filtradas 
	 GROUP BY gtin;
    
    DROP TEMPORARY TABLE IF EXISTS temp_media_cidade_gtin;
    CREATE TEMPORARY TABLE temp_media_cidade_gtin (
	 	 gtin VARCHAR(14), cidade VARCHAR(50), 
		 media_cidade_gtin DECIMAL(10,4) NULL, 
		 PRIMARY KEY (gtin, cidade)) 
	    ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
		 
    INSERT INTO temp_media_cidade_gtin (gtin, cidade, media_cidade_gtin) 
	 SELECT gtin, cidade, AVG(valor) 
	 FROM temp_notas_filtradas 
	 GROUP BY gtin, cidade;
    
    DROP TEMPORARY TABLE IF EXISTS temp_produtos_unicos;
    CREATE TEMPORARY TABLE temp_produtos_unicos (
	 	 gtin VARCHAR(14) NOT NULL, 
		 id_produto VARCHAR(20) NULL, 
		 PRODUTO TEXT NULL, PRIMARY KEY (gtin)) 
		 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
    INSERT INTO temp_produtos_unicos (gtin, id_produto, PRODUTO)
	  
	 SELECT 
	 	p.gtin AS gtin, 
		 MAX(p.id_produto) AS id_produto, 
		 MAX(TRIM(CONCAT_WS(' - ', p.gtin, NULLIF(p.descricao, ''), NULLIF(p.fabricante, '')))) AS PRODUTO 
		 FROM bronze_menorPreco_produtos p 
		 INNER JOIN (
		 	 SELECT DISTINCT gtin 
			 FROM temp_notas_filtradas 
			 WHERE gtin IS NOT NULL) AS gtin_filtrados 
			 ON p.gtin = gtin_filtrados.gtin GROUP BY p.gtin;
    
    DROP TEMPORARY TABLE IF EXISTS temp_ranks;
    CREATE TEMPORARY TABLE temp_ranks (
	 	 cidade VARCHAR(50), 
	    microrregiao VARCHAR(100), 
		 bandeira VARCHAR(100), 
		 rank_cidade INT NULL, 
		 rank_microrregiao INT NULL, 
		 PRIMARY KEY (cidade, microrregiao, bandeira), 
		 INDEX idx_cidade_bandeira (cidade, bandeira), 
		 INDEX idx_micro_bandeira (microrregiao, bandeira)) 
		 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
		 
    INSERT INTO temp_ranks (cidade, microrregiao, bandeira, rank_cidade, rank_microrregiao) 
	 WITH base_contagem AS (
	 	 SELECT n.descricao, n.cidade, n.microrregiao, l.bandeira 
		  FROM temp_notas_filtradas AS n LEFT 
		  JOIN bronze_menorPreco_lojas AS l 
		  ON n.id_loja = l.id_loja 
		  WHERE 
		  	 l.bandeira IS NOT NULL 
			 AND l.bandeira NOT IN ('MERCADO', 'OUTRAS BANDEIRAS', 'DROGAMAIS')), 
			 contagens AS (
			 	SELECT 
				 	cidade, 
					microrregiao, 
					bandeira, 
					COUNT(descricao) AS contagem 
					FROM base_contagem 
					GROUP BY cidade, microrregiao, bandeira
				) SELECT 
						cidade, 
						microrregiao, 
						bandeira, 
						DENSE_RANK() OVER (PARTITION BY cidade ORDER BY contagem DESC), 
						DENSE_RANK() OVER (PARTITION BY microrregiao ORDER BY contagem DESC) 
						FROM contagens;
    
    -- ==========================================================
    -- 4) INSERT final na Silver
    -- ==========================================================
    INSERT INTO silver_menorPreco_notas (
        id_nota, `date`, id_loja, geohash, gtin, 
        valor, valor_desconto, valor_tabela, cidade, data_atualizacao, bandeira, 
        latitude, longitude, nome_fantasia, razao_social, PRODUTO, microrregiao, 
        DateRelacionamento, rank_cidade, rank_microrregiao, considerado
    )
    SELECT
        n.id_nota, n.`date`, n.id_loja, n.geohash, n.gtin, 
        n.valor, n.valor_desconto, n.valor_tabela, n.cidade, n.data_atualizacao, 
        l.bandeira, l.latitude, l.longitude, COALESCE(l.nome_fantasia, 'NÃO ENCONTRADO'), l.razao_social, 
        COALESCE(p.PRODUTO, CONCAT(n.gtin, ' - ', n.descricao)) AS PRODUTO, 
        n.microrregiao, CAST(n.`date` AS DATE), r.rank_cidade, r.rank_microrregiao,
        CASE
            WHEN m_gtin.media_gtin IS NULL THEN NULL
            WHEN n.valor >= (m_gtin.media_gtin * 1.5) OR n.valor <= (m_gtin.media_gtin * 0.5) THEN 'N'
            ELSE 'S'
        END AS considerado
    FROM temp_notas_filtradas AS n
    LEFT JOIN bronze_menorPreco_lojas AS l ON n.id_loja = l.id_loja
    LEFT JOIN temp_produtos_unicos AS p ON n.gtin = p.gtin
    LEFT JOIN temp_media_gtin AS m_gtin ON n.gtin = m_gtin.gtin
    LEFT JOIN temp_media_cidade_gtin AS m_cid_gtin ON n.gtin = m_cid_gtin.gtin AND n.cidade = m_cid_gtin.cidade
    LEFT JOIN temp_ranks AS r ON n.cidade <=> r.cidade AND n.microrregiao <=> r.microrregiao AND l.bandeira <=> r.bandeira
    
    ON DUPLICATE KEY UPDATE
        data_atualizacao = VALUES(data_atualizacao);
    
    -- ==========================================================
    -- AUDITORIA 2: Cálculos Finais
    -- ==========================================================
    -- A. Conta quantas linhas existem agora
    SELECT COUNT(*) INTO v_qtd_depois FROM silver_menorPreco_notas;
    
    -- B. Conta quantas linhas foram TOCADAS (Data Atualizacao > Data Antiga)
    -- Isso inclui tanto os novos quanto os que só atualizaram o timestamp
    SELECT COUNT(*) INTO v_total_touched 
    FROM silver_menorPreco_notas 
    WHERE data_atualizacao > v_watermark;

    -- C. Matemática simples
    SET v_novos = v_qtd_depois - v_qtd_antes;
    SET v_alterados = v_total_touched - v_novos;

    -- Limpeza
    DROP TEMPORARY TABLE IF EXISTS temp_notas_filtradas;
    DROP TEMPORARY TABLE IF EXISTS temp_media_gtin;
    DROP TEMPORARY TABLE IF EXISTS temp_media_cidade_gtin;
    DROP TEMPORARY TABLE IF EXISTS temp_produtos_unicos;
    DROP TEMPORARY TABLE IF EXISTS temp_mapa_geo;
    DROP TEMPORARY TABLE IF EXISTS temp_ranks;
    DROP TEMPORARY TABLE IF EXISTS temp_top_gtins;

    -- ==========================================================
    -- RESULTADO FINAL NA TELA
    -- ==========================================================
    SELECT 
        v_qtd_antes AS 'Total_Antes',
        v_qtd_depois AS 'Total_Agora',
        v_novos AS 'Novos_Registros (INSERT)',
        v_alterados AS 'Registros_Atualizados (UPDATE)';

END$$

DELIMITER ;
