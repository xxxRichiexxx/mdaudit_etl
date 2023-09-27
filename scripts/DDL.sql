CREATE TABLE stage.mdaudit_questions (
	"data" text NULL,
	"period" date NULL,
	ts timestamp NULL DEFAULT now()
)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE(period) 
          (
          PARTITION jan2022 START ('2022-01-01'::date) END ('2023-01-01'::date), 
          PARTITION jan2023 START ('2023-01-01'::date) END ('2024-01-01'::date), 
          DEFAULT PARTITION default_part
          );