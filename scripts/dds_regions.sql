INSERT INTO dds.quality_of_service_regions
(
    region_id
    ,region_name
)
WITH 
    data AS
    (
        SELECT json_array_elements(data) as data
        FROM stage.mdaudit_questions
    )         
SELECT DISTINCT
	replace((data ->> 'region_id'), '"', '')::int               AS region_id
	,replace((data ->> 'region_name'), '"', '')::varchar(500)   AS region_name
FROM data
WHERE replace((data ->> 'region_id'), '"', '')::int NOT IN 
	(SELECT DISTINCT region_id FROM dds.quality_of_service_regions);

