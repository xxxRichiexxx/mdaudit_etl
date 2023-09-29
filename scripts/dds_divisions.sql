BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_divisions
WHERE id IN 
	(
        SELECT DISTINCT
	    replace((json_array_elements(data) ->> 'division_id'), '"', '')::int
        FROM stage.mdaudit_questions
    );


INSERT INTO dds.quality_of_service_divisions
(id, division_name)
WITH data AS(
	SELECT json_array_elements(data) as data
	FROM stage.mdaudit_questions
)         
SELECT DISTINCT
	replace((data ->> 'division_id'), '"', '')::int						as id
	,replace((data ->> 'division_name'), '"', '')::varchar(50)			as division_name
FROM data;

COMMIT TRANSACTION;