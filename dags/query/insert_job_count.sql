DELETE FROM job_count
WHERE search_date = '{{ ds }}'
AND country = '{{ params.country }}';

insert into job_count
(
    job_cat,
    loc,
    job_type,
    job_count,
    prev_range,
    country,
    search_date
)
select
    replace(job_cat, '+', '_'),
    replace(loc, '+', '_'),
    job_type,
    max(job_count) as job_count,
    prev_range,
    '{{ params.country }}' as country,
    search_date
from
    {{ params.country }}_job_count_raw_{{ ds_nodash }}
where
    search_date = '{{ ds }}'
group by
    job_cat,
    loc,
    job_type,
    prev_range,
    search_date
;