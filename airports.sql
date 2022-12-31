CREATE TABLE "airports" (
	`Name`	TEXT,
	`City`	TEXT,
	`Country`	TEXT,
	`IATA`	TEXT,
	`ICAO`	TEXT,
	`Latitude`	REAL,
	`Longitude`	REAL,
	`Altitude`	INTEGER,
	`Timezone`	TEXT,
	PRIMARY KEY(`ICAO`)
);
CREATE INDEX `idx_IATA` ON `Airports` (
	`IATA`
);
CREATE INDEX `idx_position` ON `Airports` (
	`Latitude`,
	`Longitude`
);
