BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_answers
WHERE check_id IN 
	(
	SELECT DISTINCT id
	FROM stage.mdaudit_checklists
    WHERE last_modified_at >= '{{execution_date.date() - params.delta}}'
        AND last_modified_at < '{{next_execution_date.date()}}'
	);

INSERT INTO dds.quality_of_service_answers
	SELECT
		replace(json_array_elements(data -> 'answers') ->> 'id', '"', '')::INT 					AS id
		,replace((data ->> 'id'), '"', '')::INT  												AS check_id
		,replace(json_array_elements(data -> 'answers') ->> 'question_id', '"', '')::INT 		AS question_id
		,replace(json_array_elements(data -> 'answers') ->> 'name', '"', '')::VARCHAR(1000) 	AS name
		,replace(json_array_elements(data -> 'answers') ->> 'answer', '"', '')::NUMERIC(6,3)	AS answer
		,replace(json_array_elements(data -> 'answers') ->> 'weight', '"', '')::INT 			AS weight
		,replace(json_array_elements(data -> 'answers') ->> 'comment', '"', '')::VARCHAR(3000)  AS comment
	FROM stage.mdaudit_checklists
    WHERE last_modified_at >= '{{execution_date.date() - params.delta}}'
        AND last_modified_at < '{{next_execution_date.date()}}';

COMMIT TRANSACTION;
