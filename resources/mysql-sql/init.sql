
CREATE SCHEMA IF NOT EXISTS research_db;
CREATE SCHEMA IF NOT EXISTS sec_master_db;
CREATE SCHEMA IF NOT EXISTS trading_db;


DROP TABLE IF EXISTS `research_db`.`signals`;



CREATE TABLE `research_db`.`signals` (
`as_of_date` date not null,
`ticker` varchar(45) not null,
`signal` tinyint(4) default null,
PRIMARY KEY (`as_of_date`, `ticker`)
) ENGINE=InnoDB;

CREATE TABLE `trading_db`.`trading_signal`s (
`as_of_date` date not null,
`ticker` varchar(45) not null,
`signal` enum ('Buy','Sell', 'Hold'),
PRIMARY KEY (`as_of_date`, `ticker`)
) ENGINE=InnoDB;


INSERT INTO `research_db`.`signals` (`as_of_date`, `ticker`, `signal`) VALUES ('2019-02-01', 'ABC1', '-1');
INSERT INTO `research_db`.`signals` (`as_of_date`, `ticker`, `signal`) VALUES ('2019-02-01', 'ABC2', '0');
INSERT INTO `research_db`.`signals` (`as_of_date`, `ticker`, `signal`) VALUES ('2019-02-01', 'ABC3', '1');
