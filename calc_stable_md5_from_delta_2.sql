DROP VIEW cv_count_events;
DROP VIEW t_time_events_add_row_id;
DROP FOREIGN TABLE s_time_events_with_row_id;
DROP FOREIGN TABLE s_time_events;

CREATE FOREIGN TABLE s_time_events (
  machine_uuid VARCHAR(255),
  type VARCHAR(255),
  value BIGINT,
  event_timestamp TIMESTAMPTZ
  ) SERVER pipelinedb;

CREATE FOREIGN TABLE s_time_events_with_row_id (
  row_id TEXT,
  machine_uuid VARCHAR(255),
  type VARCHAR(255),
  value BIGINT,
  event_timestamp TIMESTAMPTZ
  ) SERVER pipelinedb;

CREATE VIEW t_time_events_add_row_id WITH (action = transform, outputfunc=pipelinedb.insert_into_stream('s_time_events_with_row_id'))
AS
SELECT md5(ROW (machine_uuid, type)::text) as row_id,
       machine_uuid,
       type,
       value,
       event_timestamp
FROM s_time_events;

CREATE VIEW cv_count_events WITH (action = materialize, sw = '5 minute')
AS
SELECT row_id,
       machine_uuid,
       type,
       count(*),
       max(event_timestamp)
FROM s_time_events_with_row_id
GROUP BY row_id, machine_uuid, type;

INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000, current_timestamp);
INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000, current_timestamp);
INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000, current_timestamp);
INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'process', 1000, current_timestamp);
INSERT INTO s_time_events
VALUES ('4580077b-c0e1-56db-8e9c-ea47a8330d87', 'operator', 1000, current_timestamp);

SELECT row_to_json(r.*)
FROM (
       SELECT row_id, machine_uuid, type, count
       FROM cv_count_events
     ) r;

select *
from cv_count_events;
