WITH c1 AS (SELECT
	cd.CONTINENT 
	, cd.LOCATION 
	, cd.RECORD_DATE
	, cc.POP
	, cc.CODE_TYPE 
	, cd.NEW_CASES 
	, SUM(cd.NEW_CASES) OVER (PARTITION BY cd.LOCATION ORDER BY cd.LOCATION, cd.RECORD_DATE) AS TOTAL_CASES
	-- Because the column 'NEW_CASES' is not a cumulative running total - this is partitioning and then ordering to capture that cumulative total by day
	, v.NEW_VAX 
	, SUM(v.NEW_VAX) OVER (PARTITION BY cd.LOCATION ORDER BY cd.LOCATION, cd.RECORD_DATE) AS TOTAL_VAX
	-- Because the column 'NEW_VAX' is not a cumulative running total - this is partitioning and then ordering to capture that cumulative total by day
	, cd.NEW_DEATHS 
	, SUM(cd.NEW_DEATHS) OVER (PARTITION BY cd.LOCATION ORDER BY cd.LOCATION, cd.RECORD_DATE) AS TOTAL_FATALITIES
	-- Because the column 'NEW_DEATHS' is not a cumulative running total - this is partitioning and then ordering to capture that cumulative total by day
	FROM PROJECT.COVID_DATA cd 
	LEFT JOIN PROJECT.COUNTRY_CODE cc ON cd.ISO_CODE = cc.ISO_CODE 
	LEFT JOIN PROJECT.COVID_VAX v ON cd.ISO_CODE = v.ISO_CODE AND cd.RECORD_DATE = v.RECORD_DATE 
	WHERE cd.CONTINENT IS NOT NULL)

SELECT 
c1.*
, (c1.TOTAL_VAX/c1.POP)*100 AS VAX_TO_POP
, (c1.TOTAL_CASES/c1.POP)*100 AS CASES_TO_POP
, (c1.TOTAL_FATALITIES/c1.POP)*100 AS FATALITIES_TO_POP
, (c1.TOTAL_FATALITIES/c1.TOTAL_CASES)*100 AS FATALITIES_TO_CASES
-- CREATED A CTE TO SHOW PERCENTAGES USING THE COLUMNS CREATED FOR THE TOTALS
FROM c1
ORDER BY 2,3 