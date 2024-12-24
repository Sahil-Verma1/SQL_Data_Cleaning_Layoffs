-- SOURCE (https://www.kaggle.com/datasets/swaptr/layoffs-2022)

-- Reviewing Data

SELECT * FROM
layoffs;

-- Data Cleaning
#Steps to follow :-
/* 
1. Remove Duplicates
2. Standardize the Data
3. Null value or blank values
4. Remove any Columns (If requried) */

-- 1. Removing Duplicates

#Creating a new table in order to intact the original data
CREATE TABLE layoffs_staging
LIKE layoffs;

#Inserting the Data
INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT * 
FROM layoffs_staging; -- Data is ready for cleaning

#Making A Row Number Column using WINDOW FUCNTION to tackle with duplicates
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company,industry,total_laid_off,percentage_laid_off,'date') AS row_num
FROM layoffs_staging;

#using CTE to filter
WITH duplicate_cte AS
(SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

# Creating a new table with 'row_num' to protect the data before deleting duplicate values
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

# Inserting values
INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

# Deleting the Duplicates
DELETE
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2;

-- 2. Standardizing Data

# Removing the extra spaces
UPDATE layoffs_staging2
SET company = TRIM(company);

# Checking if Industry has repeated values
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT industry
FROM layoffs_staging2
WHERE industry LIKE "Crypto%";

# Fixing Crypto words repeatation
UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

# Fixing "United States."
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE "United States%";

-- Converting date from STR to Date DataType
SELECT `date`,
STR_TO_DATE(`date`,"%m/%d/%Y")
FROM layoffs_staging2;

#updaing the format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,"%m/%d/%Y");

#changing the datatype
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Dealing with null values

SELECT * 
FROM layoffs_staging2; # Noticing the null and blank values

#Checking null/blanks in industry with depth
SELECT DISTINCT industry 
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL OR industry = ""; # We have 4 values here and 3 of them are blanks

# Converting Blanks to null

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = "";

# Checking if we have more Airbnb data for industry
SELECT *
FROM layoffs
WHERE company = "Airbnb" AND location = "SF Bay Area"; # We can see another Airbnb entry in which industry is Travel

# Now we can populate the null values in Industry column
#using self join
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; # If both are null, it cannot be populated

# Populating using UPDATE command
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- 4. Deleting the Non-useful data
/*
Total_laid_off and percentage_laid_off can not be populated by me alone. Keeping them may affect the EDA so
I decided to remove them form the Dataset.
*/

#There are 348 rows of such data
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

#Deleting them
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Droping the extra row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- The data is ready to explore!!
SELECT * 
FROM layoffs_staging2







