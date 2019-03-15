<<<<<<< HEAD
-- Sums all accumulated exports for each country
SELECT country_code, SUM(accumulated_exports_current_year) AS `Total_Accumulated_Exports`
FROM `alien-grove-220420.Aggriculture.Exports_MS5`
GROUP BY country_code

-- Sums all exports by week
SELECT SUM(current_week_export) AS `Total_Exports_by_Week`, date
FROM `alien-grove-220420.Aggriculture.Exports_MS5`
GROUP BY date
ORDER BY date

-- Sums all exports by commodity code
SELECT commodity_code, SUM(current_week_export) AS `Total_Exports_by_Week`, date
FROM `alien-grove-220420.Aggriculture.Exports_MS5`
GROUP BY commodity_code, date
ORDER BY date

-- Sums All Exports to Japan
SELECT country_code, SUM(current_week_export) AS `Total_Exports_by_Week`, date
FROM `alien-grove-220420.Aggriculture.Exports_MS5`
GROUP BY country_code, date
HAVING country_code = 'JAPAN'
ORDER BY date

-- Query returns total outstanding sales by country, for counries wity greater than 1,000,000
SELECT country_code, SUM(outstanding_sales_current_year) AS `Total_Outstanding_Sales`
FROM `alien-grove-220420.Aggriculture.Exports_MS5`
GROUP BY country_code
HAVING Total_Outstanding_Sales > 1000000


-- Returns average weekly exports for each commodity
SELECT commodity_code, AVG(current_week_export) AS `Average_Commodity_Export`
FROM `alien-grove-220420.Aggriculture.Exports_MS5`
GROUP BY commodity_code

-- Returns countries with over 1000 average weekly sales
SELECT country_code, AVG(current_week_export) AS `Average_Weekly_Sales`
FROM `alien-grove-220420.Aggriculture.Exports_MS5`
GROUP BY country_code
HAVING Average_Weekly_Sales > 1000

-- Selects highest anual outstanding sales for each country
SELECT country_code, MAX(outstanding_sales_current_year) AS `Highest_Annual_Outstanding_Sales`
FROM `alien-grove-220420.Aggriculture.Exports_MS5`
GROUP BY country_code
=======
-- aggregate-queries.sql
--8 total with agg quueries
--5+ GROUP BY 
--3+ HAVING
>>>>>>> 6e0a7e9976f93e50bf4e14e231af29b9dea7b7fc
