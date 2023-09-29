BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_answers
WHERE id IN 
	(WITH 
		data AS
			(SELECT json_array_elements(data) 													AS data
			 FROM stage.mdaudit_questions)
	SELECT
		replace(json_array_elements(data -> 'answers') ->> 'id', '"', '')::int 					AS id
	FROM data);

INSERT INTO dds.quality_of_service_answers
	WITH 
		data AS
			(SELECT json_array_elements(data) 													AS data
			 FROM stage.mdaudit_questions)
	SELECT
		replace(json_array_elements(data -> 'answers') ->> 'id', '"', '')::int 					AS id
		,replace((data ->> 'id'), '"', '')::int  												AS check_id
		,replace(json_array_elements(data -> 'answers') ->> 'question_id', '"', '')::int 		AS question_id
		,replace(json_array_elements(data -> 'answers') ->> 'name', '"', '')::VARCHAR(300) 		AS name
		,replace(json_array_elements(data -> 'answers') ->> 'answer', '"', '')::VARCHAR(300) 	AS answer
		,replace(json_array_elements(data -> 'answers') ->> 'weight', '"', '')::int 			AS weight
		,replace(json_array_elements(data -> 'answers') ->> 'comment', '"', '')::VARCHAR(1000)  AS comment
	FROM data

COMMIT TRANSACTION;


