BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_checks
WHERE id IN 
	(WITH 
		data AS
			(SELECT json_array_elements(data) 							AS data
			 FROM stage.mdaudit_questions
			 WHERE period = '{{execution_date.replace(day=1)}}')
	SELECT DISTINCT
		replace(data ->>'id'), '"', '')::int 							AS id
	FROM data);

INSERT INTO dds.quality_of_service_checks
	WITH 
		data AS
			(SELECT
				json_array_elements(data) 								AS data
				,period
			 FROM stage.mdaudit_questions
			 WHERE period = '{{execution_date.replace(day=1)}}')
	SELECT
		replace(data -> 'id'), '"', '')::INT 							AS id
		,replace(data -> 'template_id'), '"', '')::INT 					AS question_i
		,replace(data -> 'shop_id'), '"', '')::INT						AS shop_id
		,replace(data -> 'division_id'), '"', '')::INT  				AS division_id
		,replace(data -> 'resolver_id'), '"', '')::INT  				AS resolver_id
		,replace(data -> 'resolve_date'), '"', '')::DATE 				AS resolve_date
		,replace(data -> 'start_time'), '"', '')::TIMESTAMP 			AS start_time
		,replace(data -> 'finish_time'), '"', '')::TIMESTAMP 			AS finish_time
		,replace(data -> 'last_modified_at'), '"', '')::TIMESTAMP 		AS last_modified_at
		,replace(data -> 'grade'), '"', '')::NUMERIC(6,3) 				AS grade
		,replace(data -> 'comment'), '"', '')::VARCHAR(1000)			AS comment
		,replace(data -> 'status'), '"', '')::VARCHAR(100)				AS status
		,period
	FROM data;

COMMIT TRANSACTION;


