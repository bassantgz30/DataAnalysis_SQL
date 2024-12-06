USE world_layoffs;

-- Show Data 
SELECT *
FROM layoffs;

/*
What to do:
	1. Standardize the Data.
	2. Remove Duplicates.
	3. Null and Blank Values.
	4. Remove any Columns/rows?
*/


-- Staging step: Creating a copy of the raw data to work on. No changes will be applied to the raw data.
DROP TABLE IF EXISTS layoffs_staging;
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- Check table
SELECT *
FROM layoffs_staging;


-- ==========================================
-- ==========================================

-- ------------------------------
-- (1) Standardize the Data.
-- ------------------------------
/* 
Issues include:
	- White Spaces (leading / trailing).
    - Inconsistancy (spelling mistakes, same text written in different ways, abbreviation).
    - Names in special characters for some languages (Chineese, German, ... )
    - Wrong data types.
*/

SELECT *
FROM layoffs_staging;

-- Remove leading and trailing spaces
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
UPDATE layoffs_staging
SET 
	company = TRIM(company),
	location = TRIM(location),
    industry = TRIM(industry),
    stage = TRIM(stage),
    country = TRIM(country);


-- Check Consistancy
-- ~~~~~~~~~~~~~~~~~~
#### 1. country
SELECT DISTINCT country
FROM layoffs_staging
ORDER BY country;

# `United States` , `United States.`  >> Same country, with different characters.
SELECT country, COUNT(country)
FROM layoffs_staging
WHERE country LIKE 'United States%'
GROUP BY country;

-- need to update.
UPDATE layoffs_staging
SET country = 'United States'
WHERE country LIKE 'United States%';

#### 2. location
SELECT DISTINCT location
FROM layoffs_staging
ORDER BY location;

# There locations in non-English letters like 'DÃ¼sseldorf'. >> Check for any other locations.
select DISTINCT location
FROM layoffs_staging
where location not regexp '^[A-Za-z .]*$'
order by location;

-- Issues
	# 'DÃ¼sseldorf'  >> 'Düsseldorf'  [City in Germany]  >>> English Spelling: Duesseldorf
	# 'FlorianÃ³polis'  >> Florianópolis  [City in Brazil] >>> English Spelling: Florianopolis
	# 'MalmÃ¶'  >> Malmö  [City in Sweden]  >>> English Spelling: Malmo

SELECT location, COUNT(location)
FROM layoffs_staging
where location not regexp '^[A-Za-z .]*$'
GROUP BY location;

-- To solve: replace the forigen spelling with their English spelling.
UPDATE layoffs_staging
SET location =
	CASE
		WHEN location = 'DÃ¼sseldorf' THEN 'Duesseldorf'
        WHEN location = 'FlorianÃ³polis' THEN 'Florianopolis'
        WHEN location = 'MalmÃ¶' THEN 'Malmo'
	END
WHERE location IN ('DÃ¼sseldorf', 'FlorianÃ³polis', 'MalmÃ¶');


#### 3. Industry
SELECT DISTINCT industry
FROM layoffs_staging
ORDER BY industry;

# Crypto, Crypto Currency , CryptoCurrency. They are all the same thing. >>>  need to update.
SELECT industry, COUNT(industry)
FROM layoffs_staging
WHERE industry LIKE 'Crypto%'
GROUP BY industry;

UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

#### 4. Company
SELECT DISTINCT company, industry
FROM layoffs_staging
ORDER BY company;

## Some companies appears ore than once with different industries.
## Some Companies wirtten in different ways.

-- 'Ada' Company
SELECT * FROM layoffs_staging
WHERE company LIKE 'Ada' OR company LIKE 'Ada %';

-- 'Ada' and 'Ada Suuport' appeares to be the same company (they have the same industry, country and location).
UPDATE layoffs_staging
SET company = 'Ada'
WHERE company = 'Ada Support';

-- 'Clearco' Company
SELECT * FROM layoffs_staging
WHERE company REGEXP 'clear[cC]o';

-- 'ClearCo' and 'Clearco' appeares to be the same company
UPDATE layoffs_staging
SET company = 'Clearco'
WHERE company REGEXP 'clear[cC]o';

-- 'Lido' and ''Lido Learning'
SELECT * FROM layoffs_staging
WHERE company LIKE 'Lido%';

-- 'Lido' and ''Lido Learning' appeares to be the same company (they have the same industry, country and location).
UPDATE layoffs_staging
SET company = 'Lido Learning'
WHERE company LIKE 'Lido%';

-- Check if there are any company names wirtten in non-English letters
SELECT * FROM layoffs_staging
WHERE company NOT REGEXP '^[A-Za-z0-9 .]*$'
ORDER BY company;

SELECT * FROM layoffs_staging
WHERE company = 'UalÃ¡';  -- Uala

UPDATE layoffs_staging
SET company = 'Uala'
WHERE company = 'UalÃ¡';

    
-- wrong data types
-- ~~~~~~~~~~~~~~~~~~
# date column has a type of text
SELECT `date`,
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging;

UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date` FROM layoffs_staging;

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;

-- ==========================================
-- ==========================================

-- ------------------------------
-- (2) Check Duplicated Entries
-- ------------------------------

# With ROW_NUMBER() function and partiotioning by all columns, duplicated entries will havve valus > 1 at the row_num col
SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
FROM
	layoffs_staging;

# Extracting the duplicated entries only, (having row_num > 1)
with t as (SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
FROM
	layoffs_staging)
SELECT * FROM t WHERE row_num > 1;

# Create a new staging table with extra column 'row_num' to be able to delete rows
DROP TABLE IF EXISTS layoffs_staging_2;

CREATE TABLE `layoffs_staging_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` date,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging_2
SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, total_laid_off, 
			percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging_2;  -- 2356 entries

SELECT * FROM layoffs_staging_2
WHERE row_num > 1;

DELETE 
FROM layoffs_staging_2
WHERE row_num > 1;

-- ==========================================
-- ==========================================

-- ------------------------------
-- (3) Null and Blank values.
-- ------------------------------

SELECT * FROM layoffs_staging_2;

-- Update text cols, set all blank values to null. It's easier to work with.
UPDATE layoffs_staging_2
SET company = NULL WHERE company = '';

UPDATE layoffs_staging_2
SET location = NULL WHERE location = '';

UPDATE layoffs_staging_2
SET industry = NULL WHERE industry = '';

UPDATE layoffs_staging_2
SET	percentage_laid_off = NULL WHERE percentage_laid_off = '';

UPDATE layoffs_staging_2
SET stage = NULL WHERE stage = '';

UPDATE layoffs_staging_2
SET country = NULL WHERE country = '';

-- Count the NULL values for all columns
SELECT 
	COUNT(*)-COUNT(company) As company, 
    COUNT(*)-COUNT(location) As location, 
    COUNT(*)-COUNT(industry) As industry,
	COUNT(*)-COUNT(total_laid_off) As total_laid_off, 
    COUNT(*)-COUNT(percentage_laid_off) As percentage_laid_off,
    COUNT(*)-COUNT(`date`) As `date`,
	COUNT(*)-COUNT(stage) As stage,
	COUNT(*)-COUNT(country) As country, 
    COUNT(*)-COUNT(funds_raised_millions) As funds_raised_millions
FROM layoffs_staging_2;
   

-- look at industry column
SELECT * FROM layoffs_staging_2 WHERE industry IS NULL;  -- Airbnb, Bally's Interactive, Carvana, Juul

# Airbnb
SELECT * FROM layoffs_staging_2 WHERE company = 'Airbnb';  -- 2 entries

# Bally's Interactive
SELECT * FROM layoffs_staging_2 WHERE company = "Bally's Interactive";  -- 1 entry

# Carvana
SELECT * FROM layoffs_staging_2 WHERE company = "Carvana";  -- 3 entries

# Juul
SELECT * FROM layoffs_staging_2 WHERE company = "Juul";  -- 2 entry


# To replace NULL industries, we need to make sure that the comapny exists in the same countey and same location

-- Fetch all first with self join
-- query below get all details from t1 where the industry is NULL, with all details from t2 where industry is NOT NULL (for the same company).
SELECT t1.*, t2.* 
FROM layoffs_staging_2 t1
JOIN layoffs_staging_2 t2
	ON t1.company = t2.company
    AND t1.country = t2.country
    AND t1.location = t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

# Filling the missing values
UPDATE layoffs_staging_2 t1
JOIN layoffs_staging_2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
    AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NUll;

# Company (Bally's Interactive) appears only once, with no industry info. we can change it to other/Unknown or search the web.
UPDATE layoffs_staging_2
SET industry = 'Unkonwn'
WHERE industry IS NULL;

-- look at stage column
SELECT *
FROM layoffs_staging_2
WHERE stage IS NULL ;

SELECT DISTINCT stage
FROM layoffs_staging_2
ORDER BY stage;

# There is a categor 'Unknown'. We can repalce the null values with 'Unknown'
UPDATE layoffs_staging_2
SET stage = 'Unkonwn'
WHERE stage IS NULL;


-- We don't have info about funds, may check the web or leave it as null for now.
-- 'total_laid_off' and 'percentage_laid_off', are related and we can calculate 
-- the missing values if the total number of employees is known, for now, we don't have enoght info about them, 
-- may leave them for now. and check during analysis.

# In the case both 'total_laid_off' and 'percentage_laid_off' are missing, we may delete those entries, or try to provide the missing info.
SELECT * 
FROM layoffs_staging_2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL;  -- 361 entries


-- check the tade column
SELECT *
FROM layoffs_staging_2
WHERE `date` IS NULL;

SELECT *
FROM layoffs_staging_2
WHERE company = 'Blackbaud';

# Only one entry, and that company made on one layoffs. 
# >> if the info is public, we can get it, or set a default value (like: 9999-01-01) to represent unkown.
UPDATE layoffs_staging_2
SET `date` = '9999-01-01' 
WHERE `date` IS NULL;

-- ==========================================
-- ==========================================

-- -----------------------------------
-- (4) Remove unnecessary cols / rows
-- -----------------------------------

# Delete entries where 'total_laid_off' and 'percentage_laid_off' are both missing
DELETE
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;
    
# Delete the row_num col
ALTER TABLE layoffs_staging_2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging_2;

