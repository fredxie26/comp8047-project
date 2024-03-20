DROP TABLE IF EXISTS layoff_raw_temp;

create table layoff_raw_temp
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