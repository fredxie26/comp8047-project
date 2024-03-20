DROP TABLE IF EXISTS {{ params.country }}_job_count_raw_{{ ds_nodash }};

create table {{ params.country }}_job_count_raw_{{ ds_nodash }}
(
   job_cat   varchar(30),
   loc  varchar(30),
   job_type  varchar(50),
   job_count   int,
   prev_range   int,
   search_date   varchar(10)
);