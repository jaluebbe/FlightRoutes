CREATE TABLE "airlines" (
	`ICAO`	TEXT,
	`IATA`	TEXT,
	`Name`	TEXT,
	PRIMARY KEY(`ICAO`)
);
CREATE INDEX `idx_IATA` ON `airlines` (
	`IATA`
);
