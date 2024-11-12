-- DATA CLEANING AND EXPLORATION

-- Data Cleaning Project 

SELECT *
FROM layoffs
;

show table status from world_layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or Blank values
-- 4. Remove ay columns

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- 1. Remove duplicates

-- Identify them

WITH duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER( 
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num	
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;

-- Delete them

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT layoffs_staging2
SELECT *, ROW_NUMBER() OVER( 
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

--

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Other quicker way to do it: 

CREATE TABLE layoffs_staging3
LIKE layoffs_staging;

INSERT layoffs_staging3
SELECT DISTINCT *
FROM layoffs_staging;

-- NEXT : STANDARDIZING DATA

-- Trim useless spaces before company names

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry	
FROM layoffs_staging2
ORDER BY 1;

SELECT *	
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2	
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT *	
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country	
FROM layoffs_staging2
ORDER BY 1;

-- Two ways to trim the '.' : 

UPDATE layoffs_staging2	
SET country = 'United States'
WHERE country LIKE 'United States%';

UPDATE layoffs_staging2	
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


SELECT DISTINCT country	
FROM layoffs_staging2
ORDER BY 1;

-- date type formatting

SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2	
SET `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- DEAL WITH EMPTY STRINGS AND NULL VALUES

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- Let's see if we can populate some of them

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT empt.company, empt.industry, popu.industry 
FROM layoffs_staging2 as empt
JOIN layoffs_staging2 as popu
ON empt.company = popu.company AND empt.location = popu.location
WHERE (empt.industry = '' OR empt.industry IS NULL) AND (popu.industry != '' AND popu.industry IS NOT NULL)
;

UPDATE layoffs_staging2 as empt
JOIN layoffs_staging2 as popu
ON empt.company = popu.company AND empt.location = popu.location
SET empt.industry = popu.industry
WHERE (empt.industry = '' OR empt.industry IS NULL) AND (popu.industry != '' AND popu.industry IS NOT NULL)
;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

-- NEXT total laid off and percentage laid off

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
;

-- Remove useless column

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;





-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC
;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC
;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC
;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 
;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC 
;

-- Progression of layoffs (month by month rolling sum)

WITH rolling_cte AS(
SELECT LEFT(`date`,7) AS year_and_month, SUM(total_laid_off) AS monthly_total
FROM layoffs_staging2
WHERE company != 'Blackbaud'
GROUP BY LEFT(`date`,7)
ORDER BY 1 
)
SELECT year_and_month, monthly_total, SUM(monthly_total) OVER(ORDER BY year_and_month) AS monthly_rolling_total_layoffs
FROM rolling_cte
;

-- how many were laid off per year in each company

WITH comp_roll_cte AS (
SELECT company, YEAR(`date`) as year_laid_off, SUM(total_laid_off) as total_per_company_year
FROM layoffs_staging2
WHERE company != 'Blackbaud'
GROUP BY company, YEAR(`date`)
ORDER BY company
)

SELECT company, year_laid_off, total_per_company_year,
	SUM(total_per_company_year) OVER( PARTITION BY company ORDER BY year_laid_off) AS rolling_total_per_company_year
FROM comp_roll_cte
;

-- rank companies by amount of laid off people each year

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH comp_cte AS (
SELECT company, YEAR(`date`) as years, SUM(total_laid_off) as total_comp_year
FROM layoffs_staging2
WHERE company != 'Blackbaud'
GROUP BY company, YEAR(`date`)
)
SELECT years, company, total_comp_year, 
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_comp_year DESC) AS companies_lay_off_rank
FROM comp_cte
ORDER BY companies_lay_off_rank
;

-- TOP 5 companies by number of layoffs per year (2020-2023)
WITH comp_cte AS (
SELECT company, YEAR(`date`) as years, SUM(total_laid_off) as total_layoffs_per_company_year
FROM layoffs_staging2
WHERE company != 'Blackbaud'
GROUP BY company, YEAR(`date`)
), ranked AS(
SELECT years, company, total_layoffs_per_company_year, 
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_layoffs_per_company_year DESC) AS companies_layoffs_rank
FROM comp_cte
ORDER BY companies_layoffs_rank
)
SELECT *
FROM ranked
WHERE companies_layoffs_rank < 6
ORDER BY years;

-- let's do the same for industries

WITH indus_cte AS (
SELECT industry, YEAR(`date`) as years, SUM(total_laid_off) as total_layoffs_per_industry_year
FROM layoffs_staging2
WHERE `date` IS NOT NULL
GROUP BY industry, YEAR(`date`)
), 
ranked AS(
SELECT years, industry, total_layoffs_per_industry_year, 
	DENSE_RANK() OVER(PARTITION BY years ORDER BY total_layoffs_per_industry_year DESC) AS number_of_lay_off_rank_per_industry
FROM indus_cte
ORDER BY number_of_lay_off_rank_per_industry
)

SELECT *
FROM ranked
WHERE number_of_lay_off_rank_per_industry < 6
ORDER BY years
;