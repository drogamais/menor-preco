-- Definição da tabela: bronze_cidades
CREATE TABLE IF NOT EXISTS `bronze_cidades` (
  `id` int(11) NOT NULL,
  `id_IBGE` varchar(7) DEFAULT NULL,
  `id_plugpharma` varchar(10) DEFAULT NULL,
  `cidade` varchar(50) DEFAULT NULL,
  `cidade_normalizada` varchar(50) DEFAULT NULL,
  `populacao` int(11) DEFAULT NULL,
  `latitude` decimal(9,6) DEFAULT NULL,
  `longitude` decimal(9,6) DEFAULT NULL,
  `geohash` varchar(50) DEFAULT NULL,
  `microrregiao` varchar(50) DEFAULT NULL,
  `mesorregiao` varchar(50) DEFAULT NULL,
  `uf` varchar(2) DEFAULT NULL,
  `codigo_de_area` varchar(2) DEFAULT NULL,
  `id_regiao` varchar(50) DEFAULT NULL,
  `regiao` varchar(50) DEFAULT NULL,
  `contem_associado` tinyint(1) DEFAULT 0,
  `data_atualizacao_contem_associado` date DEFAULT NULL,
  `data_atualizacao_tabela` date DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
