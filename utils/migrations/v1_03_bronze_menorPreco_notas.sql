-- Definição da tabela: bronze_menorPreco_notas
CREATE TABLE IF NOT EXISTS `bronze_menorPreco_notas` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_nota` varchar(120) NOT NULL,
  `date` datetime DEFAULT NULL,
  `id_loja` varchar(50) DEFAULT NULL,
  `geohash` varchar(12) DEFAULT NULL,
  `gtin` varchar(14) DEFAULT NULL,
  `descricao` varchar(120) DEFAULT NULL,
  `valor_desconto` decimal(10,2) DEFAULT NULL,
  `valor_tabela` decimal(10,2) DEFAULT NULL,
  `valor` decimal(10,2) DEFAULT NULL,
  `cidade` varchar(50) DEFAULT NULL,
  `data_atualizacao` datetime DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_unique_id_nota` (`id_nota`),
  KEY `idx_notas_id_loja` (`id_loja`),
  KEY `idx_notas_gtin` (`gtin`),
  KEY `idx_notas_geohash` (`geohash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
