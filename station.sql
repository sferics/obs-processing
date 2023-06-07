CREATE TABLE IF NOT EXISTS `station` (
  `stationID` varchar(6) NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `stationNumber` int DEFAULT NULL,
  `blockNumber`int DEFAULT NULL,
  `shortStationName`varchar(32) DEFAULT NULL,
  `stationOrSiteName` varchar(64) DEFAULT NULL,
  `stationType` int DEFAULT NULL,
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `heightOfStationGroundAboveMeanSeaLevel` float DEFAULT NULL,
  `heightOfBarometerAboveMeanSeaLevel` float DEFAULT NULL,
  `heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform` float DEFAULT NULL,
  `instrumentationForWindMeasurement` int DEFAULT NULL,
  `typeOfInstrumentationForEvaporationMeasurement` int DEFAULT NULL,
  PRIMARY KEY (`stationID`),
  UNIQUE KEY `unique_station` (`stationID`,`stationOrSiteName`) USING BTREE,
  KEY `station_name` (`stationOrSiteName`),
  KEY `updated` (`updated`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
COMMIT;
