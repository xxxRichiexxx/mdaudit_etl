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


-- DDS --
DROP TABLE IF EXISTS dds.quality_of_service_regions;
CREATE TABLE IF NOT EXISTS dds.quality_of_service_regions(
    id SERIAL PRIMARY KEY
    ,region_id BIGINT NOT NULL UNIQUE
    ,region_name VARCHAR(500) NOT NULL
)
DISTRIBUTED REPLICATED;

DROP TABLE IF EXISTS dds.quality_of_service_shops;
CREATE TABLE IF NOT EXISTS dds.quality_of_service_shops(
    id SERIAL PRIMARY KEY,
    shop_id BIGINT NOT NULL UNIQUE,
    active BOOLEAN,
    sap VARCHAR(100),
    locality VARCHAR(1000),
    address VARCHAR(1000),
    city VARCHAR(255),
    latitude Numeric(20,10),
    longitude Numeric(20,10),
    region_id BIGINT REFERENCES sttgaz.dds_mdaudit_regions(id)
)
DISTRIBUTED REPLICATED;



CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_divisions(
    id AUTO_INCREMENT PRIMARY KEY,
    division_id BIGINT NOT NULL UNIQUE,
    division_name VARCHAR(2000) NOT NULL
);

CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_templates(
    id AUTO_INCREMENT PRIMARY KEY,
    template_id BIGINT NOT NULL UNIQUE,
    template_name VARCHAR(2000) NOT NULL
);

CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_resolvers(
    id AUTO_INCREMENT PRIMARY KEY,
    resolver_id BIGINT NOT NULL UNIQUE,
    resolver_first_name VARCHAR,
    resolver_last_name VARCHAR
);



CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_checks(
    id AUTO_INCREMENT PRIMARY KEY,
    check_id BIGINT NOT NULL UNIQUE,
    template_id BIGINT NOT NULL REFERENCES sttgaz.dds_mdaudit_templates(id),
    shop_id BIGINT NOT NULL REFERENCES sttgaz.dds_mdaudit_shops(id),
    division_id INT NOT NULL REFERENCES sttgaz.dds_mdaudit_divisions(id),
    resolver_id BIGINT NOT NULL REFERENCES sttgaz.dds_mdaudit_resolvers(id),
    resolve_date DATE,
    start_time TIMESTAMP,
    finish_time TIMESTAMP,
    last_modified_at TIMESTAMP NOT NULL,
    grade NUMERIC(6,3),
    comment VARCHAR(8000),
    status VARCHAR NOT NULL,

    CONSTRAINT stage_mdaudit_checks_unique UNIQUE(id, shop_id) 
);


CREATE TABLE IF NOT EXISTS sttgaz.dds_mdaudit_answers(
    id AUTO_INCREMENT PRIMARY KEY,
    answer_id BIGINT NOT NULL UNIQUE,
    check_id BIGINT NOT NULL REFERENCES sttgaz.dds_mdaudit_checks(id),
    question_id BIGINT NOT NULL,
    name VARCHAR(3000) NOT NULL,
    answer NUMERIC(6,3) NOT NULL,
    weight INT NOT NULL,
    comment VARCHAR(8000)
);



