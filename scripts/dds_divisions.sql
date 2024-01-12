BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_divisions
WHERE id IN 
	(
        SELECT DISTINCT
	    replace((data ->> 'division_id'), '"', '')::int
		FROM stage.mdaudit_checklists
        WHERE last_modified_at >= '{{execution_date.date() - params.delta}}'
            AND last_modified_at < '{{next_execution_date.date()}}'
    );


INSERT INTO dds.quality_of_service_divisions
(id, division_name)      
SELECT DISTINCT
	replace((data ->> 'division_id'), '"', '')::int						as id
	,replace((data ->> 'division_name'), '"', '')::varchar(50)			as division_name
FROM stage.mdaudit_checklists
WHERE last_modified_at >= '{{execution_date.date() - params.delta}}'
    AND last_modified_at < '{{next_execution_date.date()}}';

COMMIT TRANSACTION;