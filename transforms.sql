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