--cross-dataset-queries.sql. Implements six queries outlined in MS9

-- Find all countries that were import and export partners with the USA. Return country name and year of trades.
SELECT DISTINCT country_code, year
FROM Aggriculture.Exports_MS5 e
INNER JOIN Imports.Imports_MS10 i
ON e.country_code = upper(i.partner) AND EXTRACT(YEAR FROM e.date) = i.year
ORDER BY country_code

-- Find all countries that were export partners ONLY
SELECT DISTINCT country_code
FROM Aggriculture.Exports_MS5 e
WHERE e.country_code NOT IN
(SELECT upper(partner)
FROM Imports.Imports_MS10)

-- Return total imports and exports by country in 2001
SELECT country_code, SUM(e.current_week_export) as `Total_US_Exports`, SUM(i.Trade_Value__US__) as Total_US_Imports
FROM Aggriculture.Exports_MS5 e
FULL JOIN Imports.Imports_MS10 i ON e.country_code = upper(i.partner)
WHERE EXTRACT(YEAR FROM date) = 2001 and i.year = 2001
GROUP BY country_code
ORDER BY country_code

-- Return total imports and exports by country for years with import and export data available
SELECT country_code, SUM(e.current_week_export) as `Total_US_Exports`, SUM(i.Trade_Value__US__) as Total_US_Imports
FROM Aggriculture.Exports_MS5 e
FULL JOIN Imports.Imports_MS10 i ON e.country_code = upper(i.partner)
WHERE EXTRACT (YEAR FROM e.date) = 2000 or EXTRACT (YEAR FROM e.date) = 2001 or EXTRACT (YEAR FROM e.date) = 2004 or EXTRACT (YEAR FROM e.date) = 2005
GROUP BY country_code
ORDER BY country_code

-- Return total trade value (imports + exports) by country for years with import and exports data available
SELECT country_code, SUM(e.current_week_export) + SUM(i.Trade_Value__US__) as Total_Trade_Value
FROM Aggriculture.Exports_MS5 e
FULL JOIN Imports.Imports_MS10 i ON e.country_code = upper(i.partner)
WHERE EXTRACT (YEAR FROM e.date) = 2000 or EXTRACT (YEAR FROM e.date) = 2001 or EXTRACT (YEAR FROM e.date) = 2004 or EXTRACT (YEAR FROM e.date) = 2005
GROUP BY country_code
ORDER BY country_code

-- Return total imports and exports by year for years with export and import data available
SELECT year, SUM(e.current_week_export) as `Total_US_Exports`, SUM(i.Trade_Value__US__) as Total_US_Imports
FROM Aggriculture.Exports_MS5 e
FULL JOIN Imports.Imports_MS10 i ON EXTRACT(YEAR FROM e.date) = i.year
WHERE year = 2000 or year = 2001 or year = 2004 or year = 2005
GROUP BY year
ORDER BY year