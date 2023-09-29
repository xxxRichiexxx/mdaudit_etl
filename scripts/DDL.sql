---STAGE---
DROP TABLE IF EXISTS stage.mdaudit_questions;
CREATE TABLE stage.mdaudit_questions (
	"data" json NULL
	,"period" date NULL
	,ts timestamp NULL DEFAULT now()
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(period) 
          (
          PARTITION jan2022 START ('2022-01-01'::date) END ('2023-01-01'::date)
          ,PARTITION jan2023 START ('2023-01-01'::date) END ('2024-01-01'::date) 
          ,DEFAULT PARTITION default_part
          );

DROP TABLE IF EXISTS stage.mdaudit_shops;
CREATE TABLE stage.mdaudit_shops(
	"data" json NULL
	,ts timestamp NULL DEFAULT now()
)
DISTRIBUTED RANDOMLY;


-- DDS --
DROP TABLE IF EXISTS dds.quality_of_service_regions;
CREATE TABLE IF NOT EXISTS dds.quality_of_service_regions(
    id INT PRIMARY KEY
    ,region_name VARCHAR(500) NOT NULL
)
DISTRIBUTED REPLICATED;

DROP TABLE IF EXISTS dds.quality_of_service_shops;
CREATE TABLE IF NOT EXISTS dds.quality_of_service_shops(
    id INT PRIMARY KEY
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

DROP TABLE IF EXISTS dds.quality_of_service_divisions;
CREATE TABLE IF NOT EXISTS dds.quality_of_service_divisions(
    id INT PRIMARY KEY
    ,division_name VARCHAR(1000) NOT NULL
)
DISTRIBUTED REPLICATED;


DROP TABLE IF EXISTS dds.quality_of_service_templates;
CREATE TABLE IF NOT EXISTS dds.quality_of_service_templates(
    id INT PRIMARY KEY,
    template_name VARCHAR(1000) NOT NULL
)
DISTRIBUTED REPLICATED;


DROP TABLE IF EXISTS dds.quality_of_service_resolvers;
CREATE TABLE IF NOT EXISTS dds.quality_of_service_resolvers(
    id INT PRIMARY KEY
    ,resolver_first_name VARCHAR
    ,resolver_last_name VARCHAR
)
DISTRIBUTED REPLICATED;


DROP TABLE IF EXISTS dds.quality_of_service_checks;
CREATE TABLE IF NOT EXISTS dds.quality_of_service_checks(
    id INT PRIMARY KEY
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


DROP TABLE IF EXISTS dds.quality_of_service_answers;
CREATE TABLE IF NOT EXISTS dds.quality_of_service_answers(
    id INT
    ,check_id INT NOT NULL REFERENCES dds.quality_of_service_checks(id) ON DELETE CASCADE
    ,question_id INT NOT NULL
    ,name VARCHAR(1000) NOT NULL
    ,answer NUMERIC(6,3) NOT NULL
    ,weight INT NOT NULL
    ,comment VARCHAR(3000)
    
    ,constraint quality_of_service_answers_uniq UNIQUE(id, check_id)
)
DISTRIBUTED BY (check_id);



