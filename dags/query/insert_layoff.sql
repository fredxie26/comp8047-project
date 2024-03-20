DELETE FROM layoff_data;

--before 1753
--after 1647
with totel_emp as (
    select
        company,
        round(max(case when percentage_laid_off = 0 then 0 else total_laid_off / percentage_laid_off end),0) as total_employee
    from layoff_raw_temp
    where not(total_laid_off = 0 and percentage_laid_off = 0)
    and company is not null
    group by company
)
insert into layoff_data
(
   company,
   location,
   industry,
   total_laid_off,
   percentage_laid_off,
   date,
   stage,
   country,
   funds_raised
)
select
    a.company,
    a.location,
    a.industry,
    case when total_laid_off = 0 then coalesce((percentage_laid_off * total_employee),0) else total_laid_off end as total_laid_off,
    case when percentage_laid_off = 0 and coalesce(total_employee,0) <> 0 then round((total_laid_off / total_employee),0) else percentage_laid_off end as percentage_laid_off,
    a.date,
    a.stage,
    a.country,
    a.funds_raised
from layoff_raw_temp a
left join totel_emp b
on a.company = b.company