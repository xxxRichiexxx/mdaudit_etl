DELETE FROM dds.quality_of_service_shops;

INSERT INTO dds.quality_of_service_shops
(shop_id, active, sap, locality, address, city, latitude, longitude, region_id)
WITH 
    data AS
    (
        SELECT json_array_elements(data) as data
        FROM stage.mdaudit_shops
    )         
SELECT DISTINCT
	replace((data ->> 'id'), '"', '')::int                  	AS id
	,replace((data ->> 'active'), '"', '')::varchar(500)    	AS active
	,replace((data ->> 'sap'), '"', '')::varchar(500)           AS sap
	,replace((data ->> 'locality'), '"', '')::varchar(500)  	AS locality
	,replace((data ->> 'address'), '"', '')::varchar(500)       AS address
	,replace((data ->> 'city'), '"', '')::varchar(500)      	AS city
	,replace((data ->> 'latitude'), '"', '')::varchar(500)      AS latitude
	,replace((data ->> 'longitude'), '"', '')::varchar(500) 	AS longitude
FROM data



SELECT
    s.id AS shop_id,
    active,
    sap,
    locality,
    address,
    city,
    latitude,
    longitude,
    r.id AS region_id
FROM sttgaz.stage_mdaudit_shops AS s
JOIN sttgaz.dds_mdaudit_regions AS r 
 ON s.regionId = r.region_id;
 