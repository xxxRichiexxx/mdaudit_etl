BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_regions
WHERE id IN 
	(
        SELECT DISTINCT
	    replace((json_array_elements(data) ->> 'region_id'), '"', '')::int
        FROM stage.mdaudit_questions
        WHERE period = '{{execution_date.replace(day=1)}}'
    );

INSERT INTO dds.quality_of_service_regions
(
    id
    ,region_name
)
WITH 
    data AS
    (
        SELECT json_array_elements(data) as data
        FROM stage.mdaudit_questions
        WHERE period = '{{execution_date.replace(day=1)}}'
    )         
SELECT DISTINCT
	replace((data ->> 'region_id'), '"', '')::int               AS region_id
	,replace((data ->> 'region_name'), '"', '')::varchar(500)   AS region_name
FROM data;

COMMIT TRANSACTION;

