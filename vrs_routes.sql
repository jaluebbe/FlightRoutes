CREATE TABLE "flight_routes" (
  `Callsign` TEXT, 
  `OperatorIcao` TEXT, 
  `Route` TEXT, 
  PRIMARY KEY (`Callsign`)
);
CREATE INDEX `idx_OperatorIcao` ON "flight_routes" (`OperatorIcao`);
