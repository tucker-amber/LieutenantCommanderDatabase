SELECT commodity_code, SUM(current_week_export) as `Mexico_Total_Exports`
FROM `alien-grove-220420.Aggriculture.Exports_MS5` 
WHERE country_code IN 
(SELECT country_code FROM `alien-grove-220420.Aggriculture.Exports_MS5` WHERE country_code = 'MEXICO')
GROUP BY commodity_code 

