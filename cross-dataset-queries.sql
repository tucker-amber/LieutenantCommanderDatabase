--cross-dataset-queries.sql. Implements six queries outlined in MS9

-- Finds all countries that were import and export partners with the USA
SELECT DISTINCT country_code
FROM Aggriculture.Exports_MS5 e
INNER JOIN Imports.Imports_MS10 i
ON e.country_code = upper(i.partner)