SELECT * FROM sql_project.credit_card_transcations;

--  using rank
with cte as 
(select city,sum(amount) as total_spend from credit_card_transcations
group by city
),

total_spent as(
select sum(amount) as total_amount 
from credit_card_transcations
),

final_data as
 (
select *,round(((total_spend *1 / total_amount)  *100),2) as percentage_contribution,
dense_rank() over(order by total_spend desc) as rnk 
from cte
join
total_spent
on 1 =1 
order by total_spend desc)
select city,percentage_contribution from final_data
where rnk < 6
;



-- on 1 = 1   
/* When constructing SQL queries, you often need to specify conditions for joining tables together.
 If there are no specific conditions to apply, but you still need to join the tables,
 you can use 1=1 as a placeholder condition to ensure that every row in one table is matched with every row in the other table.*/

 -- Q 2- write a query to print highest spend month and amount spent in that month for each card type

with cte as (
select card_type,
month(transaction_date)as month, 
year(transaction_date) as year,
sum(amount) as total
 from credit_card_transcations
 group by 
 card_type,
month(transaction_date),
year(transaction_date) 
)

select * from 
(select *,
dense_rank() over(partition by card_type order by total desc) as rnk
 from cte ) t
 where t.rnk = 1
 
 
 
 -- 3- write a query to print the transaction details(all columns from the table) for each card type when
-- it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)
 
 with cte as (
select *,sum(amount) over(partition by card_type order by transaction_date,transaction_id) as total_spend
from credit_card_transcations
order by card_type,total_spend desc
)

select * from 
(select *,rank() over(partition by card_type order by total_spend ) as rnk
 from cte
 where total_spend >= 1000000) t 
 where t.rnk =1
 
  -- 4Q- write a query to find city which had lowest percentage spend for gold card type
select * from credit_card_transcations

with cte as 
( select city,card_type,sum(amount) as amount,
sum(case when card_type ='Gold' then amount else 0 end)as gold_amount /*The SUM() function aggregates these amounts over the groups defined by city and card_type, 
																enabling the calculation of the gold_ratio correctly. 
																Without SUM(), the query wouldn't perform the necessary aggregation, and you would not get the desired result.*/
from credit_card_transcations
group by city,card_type)
select city,
sum(gold_amount)*1/sum(amount)  as gold_ratio
from cte
group by city
having count(gold_amount) >0 and sum(gold_amount) >0
order by gold_ratio;



-- Q5- write a query to print 3 columns:  city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)


with cte as 
(select city,exp_type,sum(amount) as total_amount,         
rank() over(partition by city order by sum(amount) desc) as rnk_desc,
rank() over(partition by city order by sum(amount) ) as rnk_asc
from credit_card_transcations
group by city,exp_type
order by city)

select city,
max(case when rnk_desc=1 then exp_type end) as highest_exp_type,     -- here max, min function is not important you can use both max or both min because each  has only 
min(case when rnk_asc=1 then exp_type end) as lowest_exp_type         -- non numeric value to call that value and group by with city we are using this functions
from cte
group by city

-- Q6 6- write a query to find percentage contribution of spends by females for each expense type

select * from credit_card_transcations



select exp_type,
(sum(case when gender='f' then amount else 0 end) / sum(amount))*100   as percentage_female_contribution
from credit_card_transcations
group  by exp_type
order by percentage_female_contribution desc


-- Q7- which card and expense type combination saw highest month over month growth in Jan-2014
select * from credit_card_transcations
with cte as (
select card_type,exp_type,year(transaction_date) as year,month(transaction_date) as month,sum(amount)as total
from credit_card_transcations
group by card_type,exp_type,year(transaction_date),month(transaction_date)
)


select * from (
select *,
lag(total,1) over(partition by card_type,exp_type order by year,month) as perivious_month_spend,
(total - lag(total,1) over(partition by card_type,exp_type order by year,month)) as mom_growth
from cte)t

where t.mom_growth is not null and year=2014 and month =1
order by t.mom_growth desc
limit 1;

-- Q9- during weekends which city has highest total spend to total no of transcations ratio 

select * from credit_card_transcations;

select city,ROUND(sum(amount)/count(1),2) as ratio from credit_card_transcations
where dayname(transaction_date) in ('saturday' ,'sunday')
-- where dayofweek(transaction_date) in (1,7)                 -- 1 sunday , 7 saturday
group by city
order by ratio desc
LIMIT 1;

-- Q10- which city took least number of days to reach its 500th transaction after the first transaction in that city
select * from credit_card_transcations;

WITH CTE AS (
select *,
ROW_NUMBER() OVER(PARTITION BY CITY ORDER BY transaction_date ,transaction_id) AS RN
 from credit_card_transcations
 )
 SELECT CITY,DATEDIFF(MAX(transaction_date), MIN(transaction_date) )AS DATE_DIFF
 FROM CTE
 WHERE RN=1 OR RN=500     -- The reason for using OR instead of AND in the condition WHERE rn = 1 OR rn = 500 is because we're trying to select rows where 
						  -- the rn column matches either 1 or 500.
                          -- Using AND in this context would mean that a row must simultaneously have rn equal to both 1 and 500, which is not possible for a single row.
						  -- A row can have only one value for rn, so it can't be equal to both 1 and 500 at the same time.
 GROUP BY CITY
 HAVING COUNT(*) = 2   -- So, HAVING COUNT(*) = 2 ensures that only cities with exactly two transactions (one with rn=1 and one with rn=500) are included in the result set. 
					   -- If there are more or fewer transactions for a city, it will be excluded from the result.