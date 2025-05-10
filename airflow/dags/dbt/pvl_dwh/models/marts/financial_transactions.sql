with
	add_categories as (
	select
		cashtrails.*,
		case
			when tags in ('Income', 'Salary') then 'Earnings'
			when tags in ('Meals', 'Sweets and Drinks') then 'Eating out'
			when tags in ('Breakfast', 'Kitchen', 'Products', 'Snacks', 'Bathroom', 'Household', 'Office Supplies') then 'Groceries'
			when tags in ('Bike', 'Coffee', 'Cinema', 'Photography', 'Events', 'Pleasure', 'Reading', 'Games') then 'Hobby'
			when tags in ('Fitness', 'Archery') then 'Sport'
			when tags in ('Loans') then 'Loans'
			when tags in ('Banks', 'Bikesharing', 'Cellular', 'Haftpflichtversicherung', 'Krankenversicherung', 'Music', 'Software', 'Rechtsversicherung', 'Donations') then 'Recurring'
			when tags in ('Miete', 'Gas', 'Strom', 'Internet', 'Rundfunkbeitrag') then 'Rent'
			when tags in ('Home Improvements', 'Kitchenware', 'Clothes', 'Printing', 'Documents', 'University', 'Haircut', 'Health', 'Other', 'Travel', 'Gifts') then 'Purchases'
			when tags in ('Bad debt', 'Waste') then 'Writeoff'
			when tags in ('Transfer', 'Balance') then 'Technical'
			when group is not null then 'Special'
			else null
		end as transaction_category
	from {{ ref('cashtrails_import') }} as cashtrails
	)

select *
from add_categories