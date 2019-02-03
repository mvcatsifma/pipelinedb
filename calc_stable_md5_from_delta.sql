DROP VIEW t_count_events_to_updated_row;
DROP FUNCTION saveUpdatedCVRows;
DROP TABLE updated_cv_rows;
DROP VIEW cv_count_events;
DROP VIEW t_time_events_add_row_id;
DROP FOREIGN TABLE s_time_events_with_row_id;
DROP FOREIGN TABLE s_time_events;

CREATE FOREIGN TABLE s_time_events (
  machine_uuid VARCHAR(255),
  type VARCHAR(255),
  value bigint
  ) SERVER pipelinedb;

CREATE FOREIGN TABLE s_time_events_with_row_id (
  row_id TEXT,
  machine_uuid VARCHAR(255),
  type VARCHAR(255),
  value BIGINT
  ) SERVER pipelinedb;

CREATE VIEW t_time_events_add_row_id WITH (action = transform, outputfunc=pipelinedb.insert_into_stream('s_time_events_with_row_id'))
AS
SELECT md5(ROW (machine_uuid, type)::text) as row_id,
       machine_uuid,
       type,
       value
FROM s_time_events;

CREATE VIEW cv_count_events WITH (action = materialize, sw = '1 minute')
AS
SELECT row_id,
       machine_uuid,
       type,
       count(*)
FROM s_time_events_with_row_id
GROUP BY row_id, machine_uuid, type;

CREATE OR REPLACE FUNCTION saveUpdatedCVRows()
  RETURNS trigger AS
$$
BEGIN
  INSERT INTO updated_cv_rows
  VALUES (NEW.view_name, NEW.row_id, NEW.op, row_to_json(NEW));
  RETURN NEW;
END;
$$
  LANGUAGE plpgsql;

CREATE VIEW t_count_events_to_updated_row WITH (action = transform, outputfunc=saveUpdatedCVRows)
AS
SELECT 'cv_count_events'                                                     as view_name,
       (CASE WHEN (new) IS NOT NULL THEN 'I' ELSE 'D' END)                   AS op,
       (CASE WHEN (new) IS NOT NULL THEN (new).row_id ELSE (old).row_id END) as row_id,
       (new).machine_uuid,
       (new).type,
       (new).count
FROM output_of('cv_count_events');

CREATE TABLE updated_cv_rows
(
  view_name VARCHAR(255),
  row_id    VARCHAR(255),
  op        VARCHAR(255),
  data      JSONB
);


INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000);
INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000);
INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000);
INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000);
INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'operator', 1000);

SELECT *
FROM cv_count_events;
-- returns two rows, one with count 4 and one with count 1

SELECT *
FROM updated_cv_rows;
-- returns two row with op 'I' (insert)

-- wait for at least 1 minute
SELECT *
FROM updated_cv_rows;
-- returns four rows, two with op 'I' (insert) and two with op 'D'

-- cv_count_events	f3b18d5ff77a9d263bc62b8e3569d82f	I	{"op": "I", "type": "process", "count": 4, "row_id": "f3b18d5ff77a9d263bc62b8e3569d82f", "view_name": "cv_count_events", "machine_uuid": "4580077b-c0e1-56db-8e9c-ea47a8330d87"}
-- cv_count_events	974ebcd339d9847a5662a7dc19628069	I	{"op": "I", "type": "operator", "count": 1, "row_id": "974ebcd339d9847a5662a7dc19628069", "view_name": "cv_count_events", "machine_uuid": "4580077b-c0e1-56db-8e9c-ea47a8330d87"}
-- cv_count_events	974ebcd339d9847a5662a7dc19628069	D	{"op": "D", "type": null, "count": null, "row_id": "974ebcd339d9847a5662a7dc19628069", "view_name": "cv_count_events", "machine_uuid": null}
-- cv_count_events	f3b18d5ff77a9d263bc62b8e3569d82f	D	{"op": "D", "type": null, "count": null, "row_id": "f3b18d5ff77a9d263bc62b8e3569d82f", "view_name": "cv_count_events", "machine_uuid": null}
