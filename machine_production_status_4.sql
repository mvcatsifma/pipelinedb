CREATE FOREIGN TABLE stream (
  id varchar(255)
  )
  SERVER pipelinedb;

CREATE VIEW v1 WITH (action = materialize) AS
SELECT id
FROM stream;

CREATE VIEW v2 WITH (action = materialize) AS
SELECT keyed_max(arrival_timestamp, (new).id) AS id,
       max(arrival_timestamp)                 as ts
FROM output_of('v1');

CREATE VIEW v3 WITH (action = materialize) AS
SELECT
       (old).id as id,
       (old).ts as start,
       (new).ts as end
FROM output_of('v2') WHERE (old) IS NOT NULL;

INSERT INTO stream
VALUES ('1');
INSERT INTO stream
VALUES ('2');
INSERT INTO stream
VALUES ('3');
INSERT INTO stream
VALUES ('4');

INSERT INTO stream
VALUES ('7');
INSERT INTO stream
VALUES ('8');

INSERT INTO stream
VALUES ('10');
INSERT INTO stream
VALUES ('11');
INSERT INTO stream
VALUES ('12');

SELECT *
FROM v1;

SELECT *
FROM v2;

SELECT *
FROM v3;

DROP VIEW v3;
DROP VIEW v2;
DROP VIEW v1;
DROP FOREIGN TABLE stream;