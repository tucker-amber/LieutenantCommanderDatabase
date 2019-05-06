-- Presentation Demo Queries

--Exports
-- Average Weekly Export of Wheat, by country:
SELECT country_code, AVG(current_week_export) as `Wheat_Average_Weekly_Exports`
FROM `alien-grove-220420.Aggriculture.Exports_MS5`
WHERE commodity_code IN
(SELECT commodity_code FROM `alien-grove-220420.Aggriculture.Exports_MS5` WHERE commodity_code like '%wheat%')
GROUP BY country_code
ORDER BY Wheat_Average_Weekly_Exports DESC

-- Exports/Imports
-- Total Trade Value:
SELECT country_code, SUM(e.current_week_export) + SUM(i.Trade_Value__US__) as Total_Trade_Value
FROM `alien-grove-220420.Aggriculture.Exports_MS5` e
FULL JOIN `alien-grove-220420.Imports.Imports_MS10` i ON e.country_code = upper(i.partner)
WHERE EXTRACT (YEAR FROM e.date) = 2000 or EXTRACT (YEAR FROM e.date) = 2001 or EXTRACT (YEAR FROM e.date) = 2004 or EXTRACT (YEAR FROM e.date) = 2005
GROUP BY country_code
ORDER BY Total_Trade_Value DESC

