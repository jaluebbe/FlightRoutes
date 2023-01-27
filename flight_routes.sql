CREATE TABLE "flight_routes" (
  `Callsign` TEXT, 
  `Route` TEXT, 
  `Source` TEXT, 
  `OperatorIcao` TEXT, 
  `OperatorIata` TEXT, 
  `FlightNumber` INT, 
  `Quality` INT DEFAULT NULL, 
  `Errors` INT DEFAULT '0', 
  `UpdateTime` INT NOT NULL, 
  `ValidFrom` INT DEFAULT NULL, 
  PRIMARY KEY (`Callsign`, `Route`)
);
CREATE INDEX `idx_Callsign` ON "flight_routes" (`Callsign`);
CREATE INDEX `idx_FlightNumber` ON "flight_routes" (`OperatorIata`, `FlightNumber`);
