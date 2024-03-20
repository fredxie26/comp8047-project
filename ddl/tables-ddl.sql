drop table if exists job_count;
drop table if exists job_post_raw;
drop table if exists layoff_data;

create table job_count
(
   job_cat   varchar(30),
   loc  varchar(30),
   job_type  varchar(50),
   job_count   int,
   prev_range   int,
   country  varchar(10),
   search_date   varchar(10)
);

create table layoff_data
(
   company   varchar(50),
   location  varchar(50),
   industry  varchar(50),
   total_laid_off   int,
   percentage_laid_off   float,
   date  varchar(10),
   stage   varchar(50),
   country  varchar(50),
   funds_raised float
);


create table job_post_raw
(
   job_id   varchar(50),
   job_category   varchar(30),
   job_title   varchar(512),
   company   varchar(512),
   location   varchar(512),
   salary   varchar(512),
   country  varchar(10),
   post_date   varchar(50),
   load_date   varchar(10)
);


drop function string_split_index;
CREATE FUNCTION string_split_index
(
@String varchar(max)
,@Separator varchar(10)
)
RETURNS TABLE
AS
RETURN
(
SELECT ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS [Index_num], value AS [Character]
FROM STRING_SPLIT(@String, @Separator)
)
GO
