with data AS(
	select json_array_elements(data) as data
	from stage.mdaudit_questions
)         
select
	replace((data ->> 'id'), '"', '')::int								as id,
	replace((data ->> 'template_id'), '"', '')::int						as template_id,
--	replace((data ->> 'template_name'), '"', '')::varchar(500)			as template_name,
	replace((data ->> 'shop_id'), '"', '')::int							as shop_id,
--	replace((data ->> 'shop_sap'), '"', '')::varchar(50)				as shop_sap,
--	replace((data ->> 'shop_locality'), '"', '')::varchar(500)			as shop_locality,
	-- replace((data ->> 'region_id'), '"', '')::int						as region_id,
--	replace((data ->> 'region_name'), '"', '')::varchar(500)			as region_name,
	replace((data ->> 'division_id'), '"', '')::int						as division_id,
--	replace((data ->> 'division_name'), '"', '')::varchar(50)			as division_name,
	replace((data ->> 'resolver_id'), '"', '')::int						as resolver_id,
--	replace((data ->> 'resolver_first_name'), '"', '')::varchar(500)	as resolver_first_name,
--	replace((data ->> 'resolver_last_name'), '"', '')::varchar(500)		as resolver_last_name,
	replace((data ->> 'resolve_date'), '"', '')::date					as resolve_date,
	replace((data ->> 'start_time'), '"', '')::timestamp				as start_time,
	replace((data ->> 'finish_time'), '"', '')::timestamp				as finish_time,
	replace((data ->> 'last_modified_at'	), '"', '')::timestamp		as last_modified_at,
	replace((data ->> 'grade'), '"', '')::numeric(6,3)					as grade,
	replace((data ->> 'comment'), '"', '')								as comment,
	replace((data ->> 'status'), '"', '')::varchar(50)					as status
from data


