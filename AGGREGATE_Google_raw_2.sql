create table Google_raw as
select
order_charged_date as purchase_date,
split_part(order_number, '..',1) as order_number,
case when split_part(order_number, '..',2) != '' then (split_part(order_number, '..',2)::INTEGER +1 ) else 0 end  as recurring_plus_1,
financial_status as status,
product_title as model,
product_id as app_id,
product_type as subscription,
currency as currency,
replace(price,',','')::float as price,
replace(taxes,',','')::float as taxes,
replace(charged_amount,',','')::float as charged_amount,
replace(charged_amount,',','')::float/ber.rate as eur_amount,
country as country
from  google_raw_2 google
left join bs_exchange_rates ber
on date((TIMESTAMP 'epoch' + ber.timestamp * INTERVAL '1 Second ')) = google.order_charged_date and ber.currency = google.currency ;