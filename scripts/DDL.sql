---STAGE---
DROP TABLE IF EXISTS stage.mdaudit_checklists;
CREATE TABLE stage.mdaudit_checklists(
    "id" INT
    ,"last_modified_at" TIMESTAMP
	,"data" json
	,ts TIMESTAMP DEFAULT now()
)
DISTRIBUTED BY(id);

DROP TABLE IF EXISTS stage.mdaudit_shops;
CREATE TABLE stage.mdaudit_shops(
    "id" INT
    ,"last_modified_at" TIMESTAMP NULL
	,"data" json
	,ts TIMESTAMP DEFAULT now()
)
DISTRIBUTED BY(id);


-- DDS --
DROP TABLE IF EXISTS dds.quality_of_service_answers;
DROP TABLE IF EXISTS dds.quality_of_service_checks;
DROP TABLE IF EXISTS dds.quality_of_service_shops;
DROP TABLE IF EXISTS dds.quality_of_service_regions;
DROP TABLE IF EXISTS dds.quality_of_service_resolvers;
DROP TABLE IF EXISTS dds.quality_of_service_templates;
DROP TABLE IF EXISTS dds.quality_of_service_divisions;



CREATE TABLE IF NOT EXISTS dds.quality_of_service_regions(
    id INT UNIQUE NOT NULL
    ,region_name VARCHAR(500) NOT NULL
)
DISTRIBUTED REPLICATED;


CREATE TABLE IF NOT EXISTS dds.quality_of_service_shops(
    id INT UNIQUE NOT NULL
    ,active BOOLEAN
    ,sap VARCHAR(100)
    ,locality VARCHAR(1000)
    ,address VARCHAR(1000)
    ,city VARCHAR(255)
    ,latitude Numeric(20,10)
    ,longitude Numeric(20,10)
    ,region_id INT references dds.quality_of_service_regions(id)
)
DISTRIBUTED REPLICATED;


CREATE TABLE IF NOT EXISTS dds.quality_of_service_divisions(
    id INT UNIQUE NOT NULL
    ,division_name VARCHAR(1000) NOT NULL
)
DISTRIBUTED REPLICATED;


CREATE TABLE IF NOT EXISTS dds.quality_of_service_templates(
    id INT UNIQUE NOT NULL
    ,template_name VARCHAR(1000) NOT NULL
)
DISTRIBUTED REPLICATED;


CREATE TABLE IF NOT EXISTS dds.quality_of_service_resolvers(
    id INT UNIQUE NOT NULL
    ,resolver_first_name VARCHAR
    ,resolver_last_name VARCHAR
)
DISTRIBUTED REPLICATED;


CREATE TABLE IF NOT EXISTS dds.quality_of_service_checks(
    id INT UNIQUE NOT NULL
    ,template_id INT NOT NULL REFERENCES dds.quality_of_service_templates(id)
    ,shop_id INT NOT NULL REFERENCES dds.quality_of_service_shops(id)
    ,division_id INT NOT NULL REFERENCES dds.quality_of_service_divisions(id)
    ,resolver_id INT NOT NULL REFERENCES dds.quality_of_service_resolvers(id)
    ,resolve_date DATE
    ,start_time TIMESTAMP
    ,finish_time TIMESTAMP
    ,last_modified_at TIMESTAMP NOT NULL
    ,grade NUMERIC(6,3)
    ,comment VARCHAR(3000)
    ,status VARCHAR NOT NULL
)
DISTRIBUTED BY (id);



CREATE TABLE IF NOT EXISTS dds.quality_of_service_answers(
    id INT NOT NULL
    ,check_id INT NOT NULL REFERENCES dds.quality_of_service_checks(id) ON DELETE CASCADE
    ,question_id INT NOT NULL
    ,name VARCHAR(1000) NOT NULL
    ,answer NUMERIC(6,3) NOT NULL
    ,weight INT NOT NULL
    ,comment VARCHAR(3000)
    
    ,constraint quality_of_service_answers_uniq UNIQUE(id, check_id)
)
DISTRIBUTED BY (check_id);