with
	add_categories as (
	select
		cashtrails.*,
		-- date(transaction_datetime) as transaction_date,
		-- extract(month from transaction_datetime) as transaction_month,
		-- extract(year from transaction_datetime) as transaction_year,
		-- extract(isoweek from transaction_datetime) as transaction_week,
		case
			when tags in ('Income', 'Salary') then 'Earnings'
			when tags in ('Meals', 'Sweets and Drinks') then 'Eating out'
			when tags in ('Breakfast', 'Kitchen', 'Products', 'Snacks', 'Bathroom', 'Household', 'Office Supplies', 'Coffee') then 'Groceries'
			when tags in ('Cinema', 'Photography', 'Events', 'Pleasure', 'Reading', 'Games', 'Brewing', 'Music') then 'Hobby'
			when tags in ('Fitness', 'Archery') then 'Sport'
			when tags in ('Bicycle', 'Carsharing', 'Driving', 'Bikesharing', 'Long-distance', 'Taxi', 'Transit') then 'Transit'
			when tags in ('Loans') then 'Loans'
			when tags in ('Banks', 'Cellular', 'Haftpflichtversicherung', 'Krankenversicherung', 'Software', 'Rechtsversicherung', 'Donations') then 'Recurring'
			when tags in ('Miete', 'Gas', 'Strom', 'Internet', 'Rundfunkbeitrag') then 'Rent'
			when tags in ('Home Improvements', 'Deco', 'Tools', 'Electronics', 'Kitchenware', 'Clothes', 'Printing', 'Documents', 'University', 'Haircut', 'Health', 'Other', 'Gifts') then 'Purchases'
			when tags in ('Travelling') then 'Travel'
			when tags in ('Bad debt', 'Waste') then 'Writeoff'
			when tags in ('Transfer', 'Balance') then 'Technical'
			when transaction_group is not null then 'Special'
			else null
		end as transaction_category
	from {{ ref('stg_cashtrails_import') }} as cashtrails
	)

select *
from add_categories
	left join {{ ref('calendar') }} as calendar on date(add_categories.transaction_datetime) = calendar.date