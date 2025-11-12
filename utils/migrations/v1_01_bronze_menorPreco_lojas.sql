-- Definição da tabela: bronze_menorPreco_lojas
CREATE TABLE IF NOT EXISTS `bronze_menorPreco_lojas` (
  `id_loja` varchar(50) NOT NULL,
  `nome_fantasia` varchar(100) DEFAULT NULL,
  `razao_social` varchar(100) DEFAULT NULL,
  `logradouro` varchar(100) DEFAULT NULL,
  `latitude` decimal(10,8) DEFAULT NULL,
  `longitude` decimal(10,8) DEFAULT NULL,
  `geohash` varchar(10) DEFAULT NULL,
  `data_atualizacao` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `bandeira` varchar(100) DEFAULT 'OUTRAS BANDEIRAS',
  PRIMARY KEY (`id_loja`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
