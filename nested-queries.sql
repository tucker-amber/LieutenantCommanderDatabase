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

-- Displays greatest total exports to one region in 2005
 SELECT MAX(totalExports) as GreatestExportsToOneRegion2005
FROM
  -- Gets total exports for each region in 2005
  (
  SELECT SUM(current_week_export) as totalExports, region_code
  FROM Aggriculture.Exports_MS5 e
  JOIN Aggriculture.Countries2 c
  ON e.country_code = c.country_name
  WHERE EXTRACT(YEAR FROM e.date) = 2005
  GROUP BY region_code)

    -- Displyas region with greatest soybean imports from US in 2002
  SELECT region_code, totalExports as SoybeansImportedFromUS_2002
  FROM
  -- Gets total exports for soybeans by region in 2002
  (
  SELECT SUM(current_week_export) as totalExports, region_code
  FROM Aggriculture.Exports_MS5 e
  JOIN Aggriculture.Countries2 c
  ON e.country_code = c.country_name
  WHERE EXTRACT(YEAR FROM e.date) = 2002 and e.commodity_code LIKE '%Soybeans%'
  GROUP BY region_code)
  ORDER BY totalExports DESC
  LIMIT 1
