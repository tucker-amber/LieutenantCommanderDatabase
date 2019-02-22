-- Union tables for 2000-20005 exports
 SELECT * FROM `alien-grove-220420.Aggriculture.Exports_2000` 
 UNION ALL 
 SELECT * FROM `alien-grove-220420.Aggriculture.Exports_2001` 
 UNION ALL
 SELECT * FROM `alien-grove-220420.Aggriculture.Exports_2002` 
 UNION ALL
 SELECT * FROM `alien-grove-220420.Aggriculture.Exports_2003` 
 UNION ALL
 SELECT * FROM `alien-grove-220420.Aggriculture.Export_2004` 
 UNION ALL
 SELECT * FROM `alien-grove-220420.Aggriculture.Exports_2005` 
 
 -- Remove columns from unioned dataset
 SELECT * EXCEPT(serialid, sort_code, net_sales_for_week_next_year, outstanding_sales_next_year)
FROM `alien-grove-220420.Aggriculture.Exports_2000_to_2005` 

-- Cast week_ending_date to type DATE and rename as Date
SELECT * EXCEPT(week_ending_date), CAST(week_ending_date AS DATE) as Date
FROM `alien-grove-220420.Aggriculture.Exports_2000_to_2005_reduced`

-- Separate entity Country from entity Export Transaction by creating Country table
CREATE TABLE Aggriculture.Countries as
select distinct country_code, country_name, region_code
from Aggriculture.Exports_2000_to_2005
where country_code is not null
order by country_code

-- Remove attributes from Export Transactions table that belong to Country entity
CREATE TABLE Aggriculture.Exports_2000_to_2005 as
SELECT * EXCEPT(region_code, country_name)
FROM Aggriculture.Exports_2000_2005_Final
