CREATE FOREIGN TABLE begin_stream (
  correlation_id varchar(255),
  machine_name varchar(255),
  status varchar(255),
  event_timestamp timestamptz
  )
  SERVER pipelinedb;

CREATE FOREIGN TABLE end_stream (
  correlation_id varchar(255),
  event_timestamp timestamptz
  )
  SERVER pipelinedb;

CREATE VIEW v1 WITH (action = materialize) AS
SELECT correlation_id, machine_name, status, event_timestamp
FROM begin_stream;

CREATE VIEW v2 WITH (action = materialize) AS
SELECT correlation_id, event_timestamp
FROM end_stream;

CREATE VIEW v3 AS
SELECT machine_name, status, t1.event_timestamp AS start, t2.event_timestamp AS end
FROM v1 AS t1
LEFT JOIN v2 AS t2 ON t1.correlation_id = t2.correlation_id;

--  machine 1 -> begin PRODUCING
INSERT INTO begin_stream
VALUES ('1', 'machine1', 'PRODUCING', current_timestamp);

--  machine 2 -> begin MAINTENANCE_NEEDED
INSERT INTO begin_stream
VALUES ('2', 'machine2', 'MAINTENANCE_NEEDED', current_timestamp);

--  machine 3 -> begin PRODUCING
INSERT INTO begin_stream
VALUES ('3', 'machine3', 'PRODUCING', current_timestamp);

-- wait a few secs
-- machine 1 -> end PRODUCING
INSERT INTO end_stream
VALUES ('1', current_timestamp);

-- machine2 -> end MAINTENANCE_NEEDED
INSERT INTO end_stream
VALUES ('2', current_timestamp);

SELECT *
FROM v1;

-- result:
-- 1	machine1	PRODUCING	          2019-03-12 09:49:59.386611
-- 2	machine2	MAINTENANCE_NEEDED	2019-03-12 09:49:59.408409
-- 3	machine3	PRODUCING	          2019-03-12 09:55:33.781916


SELECT *
FROM v2;

-- result:
-- 1	2019-03-12 09:50:06.796336
-- 2	2019-03-12 09:50:06.818846

SELECT *
FROM v3;

-- result:
-- machine1	PRODUCING	          2019-03-12 09:49:59.386611	2019-03-12 09:50:06.796336
-- machine2	MAINTENANCE_NEEDED	2019-03-12 09:49:59.408409	2019-03-12 09:50:06.818846
-- machine3	PRODUCING	          2019-03-12 09:55:33.781916  <null>

DROP VIEW v1;
DROP VIEW v2;
DROP FOREIGN TABLE begin_stream;
DROP FOREIGN TABLE end_stream;
