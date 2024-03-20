select *,
case
	when try_cast(Character as Int) is null
	then cast(load_date as date)
	else DATEADD(day, -1 * try_cast(Character as Int), cast(load_date as date))
end
from job_post_raw a
CROSS APPLY string_split_index(a.post_date, ' ')
where Index_num = 1;

with today as
(
    select
        job_id,
        max(job_category) as job_category,
        max(job_title) as job_title,
        max(company) as company,
        max(location) as location,
        max(salary) as salary,
        country,
        max(post_date) as post_date,
        load_date
    from
        job_post_raw
    where load_date = '2023-12-25'
    group by
        job_id,
        country,
        load_date
),
new_records as
(
    select
        a.*
    from
        today a
    left join
        job_post_raw b
    on
        a.job_id = b.job_id
    where b.job_id is null
)
select
    job_id,
    job_category,
    job_title,
    company,
    location,
    salary,
    country,
    case
        when try_cast(Character as Int) is null
        then cast(load_date as date)
        else DATEADD(day, -1 * try_cast(Character as Int), cast(load_date as date))
    end as post_date,
    load_date
from
    new_records c
CROSS APPLY string_split_index(c.post_date, ' ')
where Index_num = 1;