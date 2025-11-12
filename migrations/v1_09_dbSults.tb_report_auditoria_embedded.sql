-- Definição da tabela: dbSults.tb_report_auditoria_embedded
CREATE TABLE IF NOT EXISTS `tb_report_auditoria_embedded` (
  `id` char(36) NOT NULL,
  `createdAt` datetime NOT NULL,
  `organizationId` char(36) DEFAULT NULL,
  `userId` char(36) DEFAULT NULL,
  `userEmail` varchar(255) DEFAULT NULL,
  `reportId` char(36) DEFAULT NULL,
  `reportName` varchar(255) DEFAULT NULL,
  `action` int(11) DEFAULT NULL,
  `actionName` varchar(100) DEFAULT NULL,
  `data_atualizacao` datetime NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
