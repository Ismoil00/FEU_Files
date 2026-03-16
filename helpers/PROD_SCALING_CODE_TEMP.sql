
157 - акай Шодибек
164 - акай Шухрат
160 - пенсионка

12 - FEU


select * from pension.pensioner
-- where main_department_id is null
-- where financing = 'special'
and updated is null;


update pension.pensioner p set
	main_department_id = 160,
	updated = p.created

select * from commons.department
where parent_id = 12











