-- Definição da tabela: bronze_menorPreco_produtos
CREATE TABLE IF NOT EXISTS `bronze_menorPreco_produtos` (
  `id_produto` varchar(20) NOT NULL DEFAULT '',
  `gtin` varchar(14) NOT NULL,
  `descricao` varchar(60) DEFAULT NULL,
  `fabricante` varchar(100) DEFAULT NULL,
  `apresentacao` varchar(100) DEFAULT NULL,
  `tipo` varchar(50) DEFAULT NULL,
  `data_insercao` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id_produto`,`gtin`,`data_insercao`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_ai_ci;
