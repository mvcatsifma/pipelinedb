CREATE FOREIGN TABLE stream (
  machine_name varchar(255),
  status varchar(255),
  event_timestamp timestamptz
  )
  SERVER pipelinedb;

CREATE VIEW v WITH (sw='24 hour') AS
SELECT minute(event_timestamp) as minute, keyed_max(event_timestamp, status) AS status, machine_name
FROM stream
GROUP BY minute, status, machine_name;

-- status change events for machine1
-- 1. PRODUCING
-- 2. MAINTENANCE_NEEDED
INSERT INTO stream VALUES('machine1', 'PRODUCING', current_timestamp);
INSERT INTO stream VALUES('machine1', 'MAINTENANCE_NEEDED', current_timestamp);

-- status change events for machine2
-- 1. MAINTENANCE_NEEDED
-- 2. PRODUCING
INSERT INTO stream VALUES('machine2', 'MAINTENANCE_NEEDED', current_timestamp);
INSERT INTO stream VALUES('machine2', 'PRODUCING', current_timestamp);

-- wait > 1 minute
-- status change events for machine1
-- 1. MAINTENANCE_NEEDED
-- 2. NOT_PRODUCING
INSERT INTO stream VALUES('machine1', 'MAINTENANCE_NEEDED', current_timestamp);
INSERT INTO stream VALUES('machine1', 'NOT_PRODUCING', current_timestamp);

SELECT * FROM v;

-- result
-- 2019-03-12 09:31:00.000000	PRODUCING	machine1
-- 2019-03-12 09:31:00.000000	MAINTENANCE_NEEDED	machine1
-- 2019-03-12 09:31:00.000000	MAINTENANCE_NEEDED	machine2
-- 2019-03-12 09:31:00.000000	PRODUCING	machine2
-- 2019-03-12 09:32:00.000000	NOT_PRODUCING	machine1
-- 2019-03-12 09:32:00.000000	MAINTENANCE_NEEDED	machine1


DROP VIEW v;
DROP FOREIGN TABLE stream;
