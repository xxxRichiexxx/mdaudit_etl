BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_regions
WHERE id IN (SELECT DISTINCT
         	    replace(data ->> 'region_id', '"', '')::int
             FROM stage.mdaudit_checklists
             WHERE last_modified_at >= {{execution_date.date() - params.delta}}
                AND last_modified_at < {{next_execution_date}});

INSERT INTO dds.quality_of_service_regions
(
    id
    ,region_name
)
SELECT DISTINCT
	replace((data ->> 'region_id'), '"', '')::int               AS region_id
	,replace((data ->> 'region_name'), '"', '')::varchar(500)   AS region_name
FROM stage.mdaudit_checklists
WHERE last_modified_at >= {{execution_date.date() - params.delta}}
    AND last_modified_at < {{next_execution_date}};

COMMIT TRANSACTION;

