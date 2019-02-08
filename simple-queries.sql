-- Countries (listed alphabetically) that bought soybeans from US in 2000
SELECT distinct country_code
FROM `alien-grove-220420.Aggriculture.Exports_2000`
WHERE commodity_code LIKE '%soybean%'
ORDER BY country_code

-- Countries (listed alphabetically) that bought soybeans from US in 2005
SELECT distinct string_field_4
FROM `alien-grove-220420.Aggriculture.Exports_2005`
WHERE string_field_1 LIKE '%soybean%'
ORDER BY string_field_4

-- Types of food/food products purchased (listed alphabetically) from US by Finland in 2002
SELECT DISTINCT (string_field_1)
FROM `alien-grove-220420.Aggriculture.Exports_2002`
WHERE string_field_5 = "FINLAND"
ORDER BY string_field_1

-- Types of food/food products (listed alphabetically) purchased by Cuba in 2001
SELECT DISTINCT (string_field_1)
FROM `alien-grove-220420.Aggriculture.Exports_2001`
WHERE string_field_5 = "CUBA"
ORDER BY string_field_1

-- Food/Food products purchased (listed alphabetically) in 2004 by unreported countries
SELECT DISTINCT (string_field_1)
FROM `alien-grove-220420.Aggriculture.Export_2004`
WHERE string_field_5 = "UNKNOWN"
ORDER BY string_field_1

-- List of large orders (identified by country name) of food/food products ordered by week (ascending)
SELECT string_field_4
FROM `alien-grove-220420.Aggriculture.Exports_2003`
WHERE int64_field_10 > 100000
ORDER BY timestamp_field_0

