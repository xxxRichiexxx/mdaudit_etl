BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_resolvers
WHERE id IN 
	(
        SELECT DISTINCT
	    replace((data ->> 'resolver_id'), '"', '')::int
        FROM stage.mdaudit_checklists
        WHERE last_modified_at >= '{{execution_date.date() - params.delta}}'
            AND last_modified_at < '{{next_execution_date.date()}}'
    );

INSERT INTO dds.quality_of_service_resolvers
(
    id
    ,resolver_first_name
    ,resolver_last_name
)         
SELECT DISTINCT
	replace((data ->> 'resolver_id'), '"', '')::int						as id
	,replace((data ->> 'resolver_first_name'), '"', '')::varchar(500)	as resolver_first_name
	,replace((data ->> 'resolver_last_name'), '"', '')::varchar(500)	as resolver_last_name
FROM stage.mdaudit_checklists
WHERE last_modified_at >= '{{execution_date.date() - params.delta}}'
    AND last_modified_at < '{{next_execution_date.date()}}';

COMMIT TRANSACTION;
