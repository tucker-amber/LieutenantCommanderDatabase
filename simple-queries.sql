-- Countries (listed alphabetically) that bought soybeans from US in 2000
SELECT distinct country_code
FROM `Aggriculture.Exports_2000_to_2005` 
WHERE commodity_code LIKE '%soybean%'
AND EXTRACT(year FROM Date) = 2000
ORDER BY country_code

-- Countries (listed alphabetically) that bought soybeans from US in 2005
SELECT distinct country_code
FROM Aggriculture.Exports_2000_to_2005 
WHERE commodity_code LIKE '%soybean%'
AND EXTRACT(year FROM Date) = 2005
ORDER BY country_code

-- Types of food/food products purchased (listed alphabetically) from US by Finland in 2002
SELECT DISTINCT commodity_code
FROM Aggriculture.Exports_2000_to_2005 a
LEFT JOIN Aggriculture.Countries c
ON a.country_code = c.country_code
WHERE c.country_name = "FINLAND"
AND EXTRACT(year FROM a.Date) = 2002
ORDER BY a.commodity_code

-- Types of food/food products (listed alphabetically) purchased by Cuba in 2001
SELECT DISTINCT commodity_code
FROM Aggriculture.Exports_2000_to_2005 a
LEFT JOIN Aggriculture.Countries c
ON a.country_code = c.country_code
WHERE c.country_name = "CUBA"
AND EXTRACT(year FROM a.Date) = 2001
ORDER BY a.commodity_code

-- Food/Food products purchased (listed alphabetically) in 2004 by unreported countries
SELECT DISTINCT commodity_code
FROM Aggriculture.Exports_2000_to_2005 a
LEFT JOIN Aggriculture.Countries c
ON a.country_code = c.country_code
WHERE c.country_name = "UNKNOWN"
AND EXTRACT(year FROM a.Date) = 2004
ORDER BY a.commodity_code

-- List of large orders IN 2003 (identified by country name) of food/food products ordered by week (ascending)
SELECT country_code
FROM Aggriculture.Exports_2000_to_2005 a
WHERE net_sales_for_week_current_year > 100000
AND EXTRACT(year FROM a.Date) = 2003
ORDER BY Date

