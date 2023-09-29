BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_templates
WHERE id IN 
	(
        SELECT DISTINCT
	    replace((json_array_elements(data) ->> 'template_id'), '"', '')::int
        FROM stage.mdaudit_questions
    );

INSERT INTO dds.quality_of_service_templates
(
    id
    ,template_name
)
WITH 
    data AS
    (
        SELECT json_array_elements(data) as data
        FROM stage.mdaudit_questions
    )         
SELECT DISTINCT
	replace((data ->> 'template_id'), '"', '')::int						as id
	,replace((data ->> 'template_name'), '"', '')::varchar(500)			as template_name
FROM data;

COMMIT TRANSACTION;
