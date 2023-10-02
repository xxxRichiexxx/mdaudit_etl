BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_resolvers
WHERE id IN 
	(
        SELECT DISTINCT
	    replace((json_array_elements(data) ->> 'resolver_id'), '"', '')::int
        FROM stage.mdaudit_questions
        WHERE period = '{{execution_date.replace(day=1)}}'
    );

INSERT INTO dds.quality_of_service_resolvers
(
    id
    ,resolver_first_name
    ,resolver_last_name
)
WITH 
    data AS
    (
        SELECT json_array_elements(data) as data
        FROM stage.mdaudit_questions
        WHERE period = '{{execution_date.replace(day=1)}}'
    )         
SELECT DISTINCT
	replace((data ->> 'resolver_id'), '"', '')::int						as id
	,replace((data ->> 'resolver_first_name'), '"', '')::varchar(500)	as resolver_first_name
	,replace((data ->> 'resolver_last_name'), '"', '')::varchar(500)	as resolver_last_name
FROM data;

COMMIT TRANSACTION;
