BEGIN TRANSACTION;

DELETE FROM dds.quality_of_service_shops
WHERE id IN 
	(
        SELECT DISTINCT
	    replace((json_array_elements(data) ->> 'id'), '"', '')::int
        FROM stage.mdaudit_shops
    );

INSERT INTO dds.quality_of_service_shops
(id, active, sap, locality, address, city, latitude, longitude, region_id)
WITH 
    data AS
    (
        SELECT json_array_elements(data) as data
        FROM stage.mdaudit_shops
    )         
SELECT DISTINCT
	replace((data ->> 'id'), '"', '')::int                  	AS id
	,replace((data ->> 'active'), '"', '')::bool   				AS active
	,replace((data ->> 'sap'), '"', '')::varchar(500)           AS sap
	,replace((data ->> 'locality'), '"', '')::varchar(500)  	AS locality
	,replace((data ->> 'address'), '"', '')::varchar(500)       AS address
	,replace((data ->> 'city'), '"', '')::varchar(500)      	AS city
	,replace((data ->> 'latitude'), '"', '')::numeric(20,10)    AS latitude
	,replace((data ->> 'longitude'), '"', '')::numeric(20,10)	AS longitude
    ,replace((data ->> 'regionId'), '"', '')::int               AS region_id
FROM data AS d;

COMMIT TRANSACTION;