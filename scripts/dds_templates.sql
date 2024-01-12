BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_templates
WHERE id IN 
	(
        SELECT DISTINCT
	    replace((data ->> 'template_id'), '"', '')::int
        FROM stage.mdaudit_checklists
        WHERE last_modified_at >= '{{execution_date.date() - params.delta}}'
            AND last_modified_at < '{{next_execution_date.date()}}'
    );

INSERT INTO dds.quality_of_service_templates
(
    id
    ,template_name
) 
SELECT DISTINCT
	replace((data ->> 'template_id'), '"', '')::int						as id
	,replace((data ->> 'template_name'), '"', '')::varchar(500)			as template_name
FROM stage.mdaudit_checklists
WHERE last_modified_at >= '{{execution_date.date() - params.delta}}'
    AND last_modified_at < '{{next_execution_date.date()}}';

COMMIT TRANSACTION;
