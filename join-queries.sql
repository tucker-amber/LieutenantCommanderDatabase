-- Countries that bought soybeans from US in 2000 or 2001
SELECT DISTINCT a.country_code
FROM Aggriculture.Exports_2000_to_2005 a
WHERE a.commodity_code LIKE '%soybean%'
AND (EXTRACT(YEAR FROM a.Date) = 2000
OR EXTRACT(YEAR FROM a.Date) = 2001)

-- Unique items exported from 2000 to 2001
SELECT DISTINCT commodity_code, country_code  
FROM Aggriculture.Exports_2000_to_2005 a
WHERE (EXTRACT(YEAR FROM a.Date) = 2000
OR EXTRACT(YEAR FROM a.Date) = 2001)

-- Every country exported to from 2000 to 2001
SELECT DISTINCT country_code 
FROM Aggriculture.Exports_2000_to_2005 a 
WHERE (EXTRACT(YEAR FROM a.Date) = 2000
OR EXTRACT(YEAR FROM a.Date) = 2001)

-- Records in 2000 or 2001 that have matching serial id's 
-- Depricated, dropped serial id from tables

-- All records for wheat exports from 2000 to 20001
SELECT a.week_ending_date, a.commodity_code, a.country_code
FROM Aggriculture.Exports_2000_to_2005 a
WHERE a.commodity_code LIKE '%wheat%'
AND (EXTRACT(YEAR FROM a.Date) = 2000
OR EXTRACT(YEAR FROM a.Date) = 2001)

-- Weekly export values for commodities exported to Guam from 2000 to 2001
SELECT a.commodity_code, a.current_week_export
FROM Aggriculture.Exports_2000_to_2005 a
WHERE a.country_code like '%GUAM'
AND (EXTRACT(YEAR FROM a.Date) = 2000
OR EXTRACT(YEAR FROM a.Date) = 2001)
