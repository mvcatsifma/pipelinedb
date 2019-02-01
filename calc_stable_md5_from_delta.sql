DROP VIEW time_events;
CREATE FOREIGN TABLE time_events (
  machine_uuid VARCHAR(255),
  type VARCHAR(255),
  value bigint
  ) SERVER pipelinedb;

DROP VIEW cv_count_events;
CREATE VIEW cv_count_events WITH (action = materialize)
AS
SELECT machine_uuid,
       type,
       count(*)
FROM time_events
GROUP BY machine_uuid, type;

DROP FUNCTION updateCVS;
CREATE OR REPLACE FUNCTION updateCVS()
  RETURNS trigger AS
$$
BEGIN
  IF (old) IS null THEN
    -- INSERT
    INSERT INTO updated_cvs
    VALUES (md5(ROW((new).machine_uuid, (new).type)::text), 'I', row_to_json((new)));
  ELSEIF (new) IS NULL THEN
    -- DELETE
    INSERT INTO updated_cvs
    VALUES (md5(ROW((new).machine_uuid, (new).type)::text), 'D', null);
  ELSE
    -- UPDATE
    INSERT INTO updated_cvs
    VALUES (md5(ROW((new).machine_uuid, (new).type)::text), 'U', row_to_json((new)));
  END IF;
  RETURN NEW;
END;
$$
  LANGUAGE plpgsql;

DROP VIEW t_count_events;
CREATE VIEW t_count_events
  WITH (action = transform, outputfunc=updateCVS('cv_count_events'))
AS
SELECT (new).machine_uuid, (new).type, (new).count
FROM output_of('cv_count_events');

CREATE TABLE updated_cvs
(
  view_name VARCHAR(255),
  row_md5   VARCHAR(255),
  op        VARCHAR(255),
  data      JSONB
);

SELECT *
FROM cv_count_events;

SELECT *
FROM updated_cvs;

INSERT INTO time_events VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000);
INSERT INTO time_events VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000);
INSERT INTO time_events VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000);
INSERT INTO time_events VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000);