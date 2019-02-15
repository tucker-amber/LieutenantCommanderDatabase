-- Countries that bought soybeans from US in 2000 or 2001
SELECT DISTINCT country_code, string_field_4
FROM `alien-grove-220420.Aggriculture.Exports_2000` 
JOIN `alien-grove-220420.Aggriculture.Exports_2001` 
ON country_code = string_field_4
WHERE commodity_code LIKE '%soybean%'

-- Every unique item exported to each country
SELECT DISTINCT commodity_code, country_code  
FROM `alien-grove-220420.Aggriculture.Exports_2000` 
JOIN `alien-grove-220420.Aggriculture.Exports_2001` 
ON country_code = string_field_4

-- Every country exported to in 2000 or 2001
SELECT DISTINCT country_code 
FROM `alien-grove-220420.Aggriculture.Exports_2000` 
FULL JOIN `alien-grove-220420.Aggriculture.Exports_2001` 
ON serialid = int64_field_13

-- Records in 2000 or 2001 that have matching serial id's 
SELECT DISTINCT week_ending_date, commodity_code, country_code, timestamp_field_0, string_field_1, string_field_4
FROM `alien-grove-220420.Aggriculture.Exports_2000` 
FULL JOIN `alien-grove-220420.Aggriculture.Exports_2001` 
ON serialid = int64_field_13

-- All records for wheat exports in 2000 or 20001
SELECT week_ending_date, timestamp_field_0, commodity_code, country_code, string_field_1, string_field_4
FROM `alien-grove-220420.Aggriculture.Exports_2000` 
FULL JOIN `alien-grove-220420.Aggriculture.Exports_2001`
ON week_ending_date = timestamp_field_0
WHERE commodity_code LIKE '%wheat%' OR string_field_1 LIKE '%wheat%'
ORDER BY week_ending_date

-- Weekly export values for commodities exported to Guam in 2000 or 2001
SELECT commodity_code, current_week_export, string_field_1, int64_field_6
FROM `alien-grove-220420.Aggriculture.Exports_2000` 
FULL JOIN `alien-grove-220420.Aggriculture.Exports_2001`
ON country_code = string_field_4
WHERE country_code LIKE '%GUAM%' OR string_field_4 LIKE '%GUAM%'
ORDER BY week_ending_date