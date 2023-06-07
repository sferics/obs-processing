CREATE TABLE IF NOT EXISTS `obs` (
  `obsID` int NOT NULL AUTO_INCREMENT,
  `stID` varchar(255) NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `year` int NOT NULL,
  `month` int NOT NULL,
  `day` int NOT NULL,
  `hour` int NOT NULL,
  `minute` int NOT NULL,
  `timeSignificance` int DEFAULT NULL,
  `timePeriod` int DEFAULT NULL,
  PRIMARY KEY (`obsID`),
  UNIQUE KEY `unique_obs` (`obsID`,`stID`,`year`,`month`,`day`,`hour`,`minute`,`timeSignificance`,`timePeriod`) USING BTREE,
  KEY `year` (`year`),
  KEY `month` (`month`),
  KEY `day` (`day`),
  KEY `hour` (`hour`) USING BTREE,
  KEY `minute` (`minute`),
  KEY `stID` (`stID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
COMMIT;
