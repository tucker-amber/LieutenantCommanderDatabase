-- Sum all exports to Mexico by commodity_code
SELECT commodity_code, SUM(current_week_export) as `Mexico_Total_Exports`
FROM `alien-grove-220420.Aggriculture.Exports_MS5` 
WHERE country_code IN 
(SELECT country_code FROM `alien-grove-220420.Aggriculture.Exports_MS5` WHERE country_code = 'MEXICO')
GROUP BY commodity_code 

-- Sum exports of soybean products by country
SELECT country_code, SUM(current_week_export) as `Soybeans_Total_Exports`
FROM `alien-grove-220420.Aggriculture.Exports_MS5` 
WHERE commodity_code IN 
(SELECT commodity_code FROM `alien-grove-220420.Aggriculture.Exports_MS5` WHERE commodity_code like '%Soybeans%')
GROUP BY country_code 

-- Average weekly exports of wheat by country
SELECT country_code, AVG(current_week_export) as `Wheat_Average_Weekly_Exports`
FROM `alien-grove-220420.Aggriculture.Exports_MS5` 
WHERE commodity_code IN 
(SELECT commodity_code FROM `alien-grove-220420.Aggriculture.Exports_MS5` WHERE commodity_code like '%wheat%')
GROUP BY country_code 

-- Average annual outstanding sales for countries with greater than 100000 outstanding
SELECT country_code, AVG(outstanding_sales_current_year) 
FROM `alien-grove-220420.Aggriculture.Exports_MS5` 
WHERE country_code IN 
(SELECT country_code FROM `alien-grove-220420.Aggriculture.Exports_MS5` WHERE outstanding_sales_current_year > 100000)
GROUP BY country_code 

-- Displays sum of purchases in 2005 from all countries in region 1
SELECT SUM(current_week_export) AS TotalPurchasesForRegion1_2005
FROM Aggriculture.Exports_MS5 e
WHERE EXTRACT(YEAR FROM e.date) = 2005
  AND e.country_code IN
  -- Returns country codes from all countries in region 1
  (SELECT e.country_code
  FROM Aggriculture.Exports_MS5 e
  JOIN Aggriculture.Countries2 c
  ON e.country_code = c.country_name
  WHERE region_code = 1)
