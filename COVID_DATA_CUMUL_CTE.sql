WITH c1 AS (SELECT
	cd.ISO_CODE 
	, cd.CONTINENT 
	, cd.LOCATION 
	, cd.RECORD_DATE
	, cc.POP
	, cc.CODE_TYPE 
	, cd.NEW_CASES 
	, SUM(cd.NEW_CASES) OVER (PARTITION BY cd.LOCATION ORDER BY cd.LOCATION, cd.RECORD_DATE) AS CASES_ROLLING_SUM
	-- Because the column 'NEW_CASES' is not a cumulative running total - this is partitioning and then ordering to capture that cumulative total by day
	, v.NEW_VAX 
	-- Note that vaccinations that require multiple rounds are separated out. So vaccines that require 2 rounds of shots would count as 2 in this column
	, SUM(v.NEW_VAX) OVER (PARTITION BY cd.LOCATION ORDER BY cd.LOCATION, cd.RECORD_DATE) AS VAX_ROLLING_SUM
	-- Because the column 'NEW_VAX' is not a cumulative running total - this is partitioning and then ordering to capture that cumulative total by day
	, v.NEW_PPL_VXXD_CLND
	-- People vaxxed is likely a better metric for things like population % since it's not duplicating based on vaccines that require multiple rounds
	, SUM(v.NEW_PPL_VXXD_CLND) OVER (PARTITION BY cd.LOCATION ORDER BY cd.LOCATION, cd.RECORD_DATE) AS PPL_VAX_ROLLING_SUM
	, cd.NEW_DEATHS 
	, SUM(cd.NEW_DEATHS) OVER (PARTITION BY cd.LOCATION ORDER BY cd.LOCATION, cd.RECORD_DATE) AS DEATHS_ROLLING_SUM
	-- Because the column 'NEW_DEATHS' is not a cumulative running total - this is partitioning and then ordering to capture that cumulative total by day
	FROM PROJECT.COVID_DATA cd 
	LEFT JOIN PROJECT.COUNTRY_CODE cc ON cd.ISO_CODE = cc.ISO_CODE 
	LEFT JOIN PROJECT.COVID_VAX v ON cd.ISO_CODE = v.ISO_CODE AND cd.RECORD_DATE = v.RECORD_DATE 
	WHERE cc.CODE_TYPE = 'SINGULAR'
	-- The raw data source proactively groups some of the data (such as continents, income types, etc.) 
	---The COUNTRY_CODE is a dim table that shows population as well as separates these grouped rows by marking them as "CUMULATIVE", which is why I'm filtering to "SINGULAR"
	)

SELECT 
c1.*
, (c1.VAX_ROLLING_SUM/c1.POP)*100 AS VAX_TO_POP
, (c1.PPL_VAX_ROLLING_SUM/c1.POP)*100 AS PPL_VAX_TO_POP
, (c1.CASES_ROLLING_SUM/c1.POP)*100 AS CASES_TO_POP
, (c1.DEATHS_ROLLING_SUM/c1.POP)*100 AS DEATHS_TO_POP
, (c1.DEATHS_ROLLING_SUM/c1.CASES_ROLLING_SUM)*100 AS DEATHS_TO_CASES
-- CREATED A CTE TO SHOW PERCENTAGES USING THE COLUMNS CREATED FOR THE TOTALS
FROM c1
ORDER BY 2,3 