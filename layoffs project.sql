-- data cleaning

select *
from layoffs;
-- 1 remove duplicates
-- 2 standardize data(remove errors in spellings etc)
-- 3 null values or blank values
-- 4 remove any columns (which is irrelevant but not from data set)

-- we create staging table since we shud not directly modify raw data
create table layoffs_staging
like layoffs;

insert layoffs_staging
select *
from layoffs;

-- 1st step 
-- 1a identifying duplicate rows
with duplicate_cte as
(select *,
row_number() over(
partition by company, location, industry,
total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;
-- 1b now we need to identify the exact duplicate rows we want to remove
-- u can't edit a cte 
-- so we'll create a table having same data nd extra row row_num and deleting where row=2

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry,
total_laid_off, percentage_laid_off,
`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where row_num>1;

delete
from layoffs_staging2
where row_num>1;

-- standardizing data(finding issues and fixing it)

select distinct(trim(company))
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

-- issue: crypto nd cryptocurrency are same 

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry='Crypto'
where industry like 'Crypto%';

-- issue: united states nd united states.

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country=trim(trailing '.' from country)
where country like 'United States%';

-- issue:date format is text
select `date`
from layoffs_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`=str_to_date(`date`, '%m/%d/%Y')
where date!= null;

select `date`
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

-- 3.dealing with null values
select *
from layoffs_staging2
where total_laid_off ='NULL'
and percentage_laid_off='NULL';
--    we'll remove ^ these in 4th step

select *
from layoffs_staging2
where industry =NULL
OR industry ='';

update layoffs_staging2
set industry=NULL
where industry='' 
or industry='NULL';

select industry
from layoffs_staging2
where industry is null;

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company=t2.company
   and t1.location=t2.location
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null
and t2.industry is not null;

update layoffs_staging2
set total_laid_off= null
where total_laid_off ='NULL' or total_laid_off='';

update layoffs_staging2
set percentage_laid_off= null
where percentage_laid_off ='NULL' or percentage_laid_off='';

-- 4 removing irrelevant row nd column
select *
from layoffs_staging2
where total_laid_off ='NULL'
and percentage_laid_off='NULL';


delete
from layoffs_staging2
where total_laid_off ='NULL'
and percentage_laid_off='NULL';

-- we don't need row num anymore
alter table layoffs_staging2
drop column row_num;

select *
from layoffs_staging2;
