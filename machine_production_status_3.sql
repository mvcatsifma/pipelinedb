CREATE FOREIGN TABLE stream (
  machine_name varchar(255),
  status varchar(255),
  event_timestamp timestamptz
  )
  SERVER pipelinedb;

CREATE VIEW v1 WITH (action = materialize) AS
SELECT machine_name                       AS machine,
       keyed_max(event_timestamp, status) AS status,
       max(event_timestamp)               as start
FROM stream
GROUP BY machine_name;

CREATE VIEW v2 WITH (action = materialize) AS
SELECT (old).machine,
       (old).status as status,
       (old).start  as start,
       (new).start  as end
FROM output_of('v1') WHERE ((old)) IS NOT NULL;

CREATE VIEW v3 WITH (action = materialize) AS
SELECT (old).machine,
       (old).status                                               as status,
       extract('epoch' from (new).start - (old).start::timestamp) as duration
FROM output_of('v1') WHERE ((old)) IS NOT NULL;

--  machine 1 -> PRODUCING 12:00
INSERT INTO stream
VALUES ('machine1', 'PRODUCING', '2019-01-01 12:00');

--  machine 1 -> MAINTENANCE_NEEDE 12:05
INSERT INTO stream
VALUES ('machine1', 'MAINTENANCE_NEEDED', '2019-01-01 12:05');

--  machine 2 -> MAINTENANCE_NEEDED 11:50
INSERT INTO stream
VALUES ('machine2', 'MAINTENANCE_NEEDED', '2019-01-01 11:50');

--  machine 2 -> PRODUCING 12:30
INSERT INTO stream
VALUES ('machine2', 'PRODUCING', '2019-01-01 12:30');

--  machine 2 -> MAINTENANCE_NEEDED 12:40
INSERT INTO stream
VALUES ('machine2', 'MAINTENANCE_NEEDED', '2019-01-01 12:40');

--  machine 2 -> PRODUCING 12:40
INSERT INTO stream
VALUES ('machine2', 'PRODUCING', '2019-01-01 12:45');

SELECT *
FROM v1;

-- machine1	MAINTENANCE_NEEDED	2019-01-01 12:05:00.000000
-- machine2	PRODUCING	2019-01-01 12:45:00.000000

SELECT *
FROM v2;

-- machine1	PRODUCING	2019-01-01 12:00:00.000000	2019-01-01 12:05:00.000000
-- machine2	MAINTENANCE_NEEDED	2019-01-01 11:50:00.000000	2019-01-01 12:30:00.000000
-- machine2	PRODUCING	2019-01-01 12:30:00.000000	2019-01-01 12:40:00.000000
-- machine2	MAINTENANCE_NEEDED	2019-01-01 12:40:00.000000	2019-01-01 12:45:00.000000

SELECT *
FROM v3;

-- machine1	PRODUCING	300
-- machine2	MAINTENANCE_NEEDED	2400
-- machine2	PRODUCING	600
-- machine2	MAINTENANCE_NEEDED	300

SELECT machine, status, extract('epoch' from current_timestamp - start::timestamp) as duration
FROM v1
UNION
SELECT *
FROM v3;

-- machine1	PRODUCING	300
-- machine2	MAINTENANCE_NEEDED	2400
-- machine2	PRODUCING	6137152.347554
-- machine2	MAINTENANCE_NEEDED	300
-- machine2	PRODUCING	600
-- machine1	MAINTENANCE_NEEDED	6139552.347554

SELECT *, null as end
FROM v1
UNION
SELECT *
FROM v2;

-- machine1	PRODUCING	2019-01-01 12:00:00.000000	2019-01-01 12:05:00.000000
-- machine2	MAINTENANCE_NEEDED	2019-01-01 12:40:00.000000	2019-01-01 12:45:00.000000
-- machine1	MAINTENANCE_NEEDED	2019-01-01 12:05:00.000000
-- machine2	MAINTENANCE_NEEDED	2019-01-01 11:50:00.000000	2019-01-01 12:30:00.000000
-- machine2	PRODUCING	2019-01-01 12:45:00.000000
-- machine2	PRODUCING	2019-01-01 12:30:00.000000	2019-01-01 12:40:00.000000

DROP VIEW v3;
DROP VIEW v2;
DROP VIEW v1;
DROP FOREIGN TABLE stream;