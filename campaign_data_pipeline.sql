with
tassi as (
select DISTINCT
vg.org_cod,
sigla_country,
ex.anno,
ex.cambio AS cambio
from dataset.repository.sales vg
left join dataset.repository.exchange_rate ex on ex.org_cod = vg.org_cod
left join dataset.repository.countries sco on cast(vg.org_cod as int64) = cast(sco.org_code as int64)
where CAST(ex.cambio AS NUMERIC) <> 1
and cast(ex.anno as integer) = 2025
group by all
),


AA as (select
    RTRIM(REPLACE(tracking_code, '|', '_'), '_') as tracking_code,
cast(concat(left(CAST(date AS STRING), 8),'01') AS DATE) AS date, sum(unsubscribe) as unsubscribe,
sum(visits) as visits from `dataset.repository.Adobe_mapping_orders_tracking_code` group by 1,2),


AA2 AS (select distinct order_id_string,
    RTRIM(REPLACE(tracking_code, '|', '_'), '_') as tracking_code from `dataset.repository.Adobe_mapping_orders_tracking_code` group by 1,2),


incremental_cj AS (
SELECT *,
RANK() OVER(PARTITION BY nome_campagna, url, data_invio ORDER BY data_upload DESC) AS rank
FROM (
    SELECT
        * EXCEPT(url),
        REPLACE(SUBSTRING(url, STRPOS(url, 'utm_source=') + LENGTH('utm_source='), STRPOS(url, '&utm_medium=') - STRPOS(url, 'utm_source=') - LENGTH('utm_source=')) || '_' ||
        SUBSTRING(url, STRPOS(url, 'utm_medium=') + LENGTH('utm_medium='), STRPOS(url, '&utm_campaign=') - STRPOS(url, 'utm_medium=') - LENGTH('utm_medium=')) || '_' ||
        SUBSTRING(url, STRPOS(url, 'utm_campaign=') + LENGTH('utm_campaign='), LENGTH(url)),'|', '_') AS url
    FROM  `dataset.repository.kpi_customer_journey` 
    )
),


sel1 AS (
SELECT
nome_campagna,
execution_type,
REGEXP_REPLACE(label_padre, r'\[.*?\]\s*', '') as delivery_label
, CASE
WHEN data_invio like '%/%' THEN
CAST(CONCAT(
SPLIT(data_invio, '/') [SAFE_OFFSET(2)], '-',
SPLIT(data_invio, '/') [SAFE_OFFSET(1)], '-',
01) AS DATE)
ELSE cast(concat(left(CAST(data_invio AS STRING), 8),'01') AS DATE) END AS date
, url
, channel
, sum(cast(sent as decimal)) as sent
, sum(cast(delivered as decimal)) as delivered
, sum(cast(totalopen as decimal)) as totalopen
, sum(cast(uniqueopen as decimal)) as uniqueopen
, sum(cast(totalclick as decimal)) as totalclick
, sum(cast(uniqueclick as decimal)) as uniqueclick
FROM
incremental_cj
WHERE rank = 1
group by all
),


pre_sap_aa AS (
SELECT org_cod, customer_purchase_order_number, invoice_doc_no ,your_reference, CAST(REPLACE(accrual_dt,".","-") AS DATE) as date, SUM(CAST(netto AS FLOAT64)) AS netto
FROM `dataset.repository.sales` vg
LEFT join (SELECT DISTINCT customer_analysis, customer_analysys_des, cust_lev_cod_3 FROM dataset.repository.customer_analysis) c on c.customer_analysis = vg.customer_analysis
WHERE CAST(REPLACE(accrual_dt,".","-") AS DATE) >= '2024-07-01'
and cast(qta as decimal) > 0
GROUP BY 1,2,3,4,5
UNION ALL
SELECT org_cod, customer_purchase_order_number, invoice_doc_no_c, your_reference, CAST(LEFT(data,10) AS DATE) AS date, SUM(CAST(netto_euro_c AS FLOAT64))
FROM dataset.repository.sales_old
WHERE CAST(LEFT(data,10) AS DATE) < '2024-07-01'
and cast(qta_c as decimal) > 0
GROUP BY 1,2,3,4,5
),


sap_aa AS (
SELECT
cast(concat(left(CAST(date AS STRING), 8),'01') AS DATE) AS date,
tracking_code,
case
  when (your_reference = customer_purchase_order_number) then 'Sub first'
  when length(your_reference) < 2 then 'Stand alone'
  else 'Sub Subsequent'
  end as transaction_type,
IFNULL(sum(cast(netto as decimal))/AVG(CAST(cambio AS DECIMAL)),sum(cast(netto as decimal))) as revenue,
sum(cast(netto as decimal)) as revenue_local,
COUNT( DISTINCT invoice_doc_no) AS orders
FROM pre_sap_aa vg
left join AA2
on vg.customer_purchase_order_number = cast(AA2.order_id_string as string)
or vg.your_reference = cast(AA2.order_id_string as string)
left join tassi ON vg.org_cod = tassi.org_cod
GROUP BY all
),


sap_aa1 AS (
SELECT
cast(concat(left(CAST(date AS STRING), 8),'01') AS DATE) AS date,
tracking_code,
IFNULL(sum(cast(netto as decimal))/AVG(CAST(cambio AS DECIMAL)),sum(cast(netto as decimal))) as revenue,
sum(cast(netto as decimal)) as revenue_local,
COUNT( DISTINCT invoice_doc_no) AS orders
FROM pre_sap_aa vg
left join AA2
on vg.customer_purchase_order_number = cast(AA2.order_id_string as string)
or vg.your_reference = cast(AA2.order_id_string as string)
left join tassi ON vg.org_cod = tassi.org_cod
GROUP BY all
),

dist_cj AS (
select distinct UPPER(delivery_label) delivery_label
from sel1
),

all_data AS (
SELECT
'BQ' source,
sel1.date,
UPPER(nome_campagna) AS campaign,
brand,
UPPER(delivery_label) delivery_label,
url AS tracking_code,
case
when left(nome_campagna,2) in ('GB') then 'UK'
when left(nome_campagna,2) in ('CN') then 'FR'
when left(nome_campagna,3) in (' US') then 'US'
when left(nome_campagna,2) in ('UK', 'IT', 'DE', 'FR', 'US', 'AU') then left(nome_campagna,2)
when right(delivery_label,2) in ('GB') then 'UK'
when right(delivery_label,2) in ('CN') then 'FR'
when right(delivery_label,3) in (' US') then 'US'
when right(delivery_label,2) in ('UK', 'IT', 'DE', 'FR', 'US', 'AU') then right(delivery_label,2)
ELSE 'NOT SET' END as country,
case when channel = 'Email' then 'EMAIL' else channel end AS type,
CAST(delivered AS INT64) AS delivered,
CAST(totalopen AS INT64) AS opened,
CAST(uniqueopen AS INT64) AS unique_opened,
CAST(totalclick AS INT64) AS clicked,
CAST(uniqueclick AS INT64) AS unique_click,
revenue,
revenue_local,
orders,
unsubscribe,
visits
FROM sel1
LEFT JOIN sap_aa1
 ON REPLACE(url,"|","") = REPLACE(tracking_code,"|","") and sel1.date = sap_aa1.date
left join AA ON REPLACE(url,"|","_") = REPLACE(AA.tracking_code,"|","_") and sel1.date = AA.date
WHERE sel1.date >= '2024-07-01'

UNION ALL

SELECT
'OLD',
month,
UPPER(campaign),
brand,
UPPER(delivery_label),
tracking_code,
case
when country = 'Australia' then 'AU'
when country = 'France' then 'FR'
when country = 'Italy' then 'IT'
when country = 'Germnay' then 'DE'
when country = 'United Kingdom' then 'UK'
when country = 'United States' then 'US'
when left(campaign, 2) = 'DE' then 'DE'
ELSE 'Not Set' end as
country,
CASE when flag_campaign = 'MOBILE' THEN 'SMS'
ELSE 'EMAIL' END AS type,
sum(CAST(adobe_campaign_delivered AS DECIMAL)) AS delivered,
sum(CAST(adobe_campaign_opened AS DECIMAL)) AS opened,
sum(CAST(adobe_campaign_unique_open AS DECIMAL)) AS unique_opened,
sum(CAST(adobe_campaign_clicked AS DECIMAL)) AS clicked,
sum(CAST(adobe_campaign_unique_click AS DECIMAL)) AS unique_click,
IFNULL(sum(cast(revenue as decimal))/AVG(CAST(cambio AS DECIMAL)),sum(cast(revenue as decimal))) as revenues,
sum(cast(revenue as decimal)) as revenue_local,
sum(CAST(orders AS DECIMAL)) AS orders,
sum(CAST(unsubscribe AS DECIMAL)) AS unsubscribe,
sum(CAST(visits AS DECIMAL)) AS visits
FROM `dataset.repository.ds_crm_campaign` ds
left join tassi ON ds.country_code = tassi.sigla_country
where campaign is not null
and month < '2024-07-01'
OR (month >= '2024-07-01' and UPPER(delivery_label) not in (select DISTINCT * from dist_cj))
group by 1,2,3,4,5,6,7,8,9,10

UNION ALL --SAP RECURRENT--

SELECT
'OLD',
sap_aa1.date,
UPPER(campaign),
brand,
UPPER(dcc.delivery_label),
IFNULL(sel1.url,sap_aa1.tracking_code)tracking_code,
case
when country = 'Australia' then 'AU'
when country = 'France' then 'FR'
when country = 'Italy' then 'IT'
when country = 'Germnay' then 'DE'
when country = 'United Kingdom' then 'UK'
when country = 'United States' then 'US'
when left(campaign, 2) = 'DE' then 'DE'
ELSE 'Not Set' end as
country,
CASE when flag_campaign = 'MOBILE' THEN 'SMS'
ELSE 'EMAIL' END AS type,
CAST(delivered AS INT64) AS delivered,
CAST(totalopen AS INT64) AS opened,
CAST(uniqueopen AS INT64) AS unique_opened,
CAST(totalclick AS INT64) AS clicked,
CAST(uniqueclick AS INT64) AS unique_click,
sap_aa1.revenue,
sap_aa1.revenue_local,
sap_aa1.orders,
AA.unsubscribe,
AA.visits
FROM sel1
full JOIN sap_aa1
 ON REPLACE(sel1.url,"|","") = REPLACE(sap_aa1.tracking_code,"|","")  
left join AA ON REPLACE(sap_aa1.tracking_code,"|","_") = REPLACE(AA.tracking_code,"|","_") and sel1.date = AA.date
inner join dataset.repository.ds_crm_campaign dcc on REPLACE(sap_aa1.tracking_code,"|","_") = REPLACE(dcc.tracking_code,"|","_") 
WHERE
sap_aa1.date >= '2024-07-01'
and sel1.url is null

UNION ALL --SAP ONE SHOT MESE ATTRIBUZIONE DIVERSO

SELECT
'BQ' source,
case when transaction_type = 'Sub Subsequent' then
sap_aa.date else sel1.date end as date,
UPPER(nome_campagna) AS campaign,
brand,
UPPER(delivery_label) delivery_label,
url AS tracking_code,
case
when left(nome_campagna,2) in ('GB') then 'UK'
when left(nome_campagna,2) in ('CN') then 'FR'
when left(nome_campagna,3) in (' US') then 'US'
when left(nome_campagna,2) in ('UK', 'IT', 'DE', 'FR', 'US', 'AU') then left(nome_campagna,2)
when right(delivery_label,2) in ('GB') then 'UK'
when right(delivery_label,2) in ('CN') then 'FR'
when right(delivery_label,3) in (' US') then 'US'
when right(delivery_label,2) in ('UK', 'IT', 'DE', 'FR', 'US', 'AU') then right(delivery_label,2)
ELSE 'NOT SET' END as country,
case when channel = 'Email' then 'EMAIL' else channel end AS type,
0 AS delivered,
0 AS opened,
0 AS unique_opened,
0 AS clicked,
0 AS unique_click,
revenue,
revenue_local,
orders,
0 unsubscribe,
0 visits
FROM sel1
RIGHT JOIN sap_aa
 ON REPLACE(url,"|","") = REPLACE(tracking_code,"|","") and  sap_aa.date > sel1.date
left join AA ON REPLACE(url,"|","_") = REPLACE(AA.tracking_code,"|","_") and sel1.date = AA.date
WHERE sel1.date >= '2024-07-01' 
),

dup_campaigns AS (
SELECT DISTINCT CONCAT(date,campaign) AS key FROM (
SELECT
  source,
  date,
  campaign,
  delivery_label,
  delivered,
  revenue,
  revenue_local,
  orders,
  unsubscribe,
  visits,
  RANK() OVER(PARTITION BY date, campaign, delivered ORDER BY source DESC) AS rank
FROM all_data
) WHERE rank > 1
),

all_data_final AS (
SELECT
  *
FROM (
SELECT
    *
FROM
    all_data
WHERE
    CONCAT(date,campaign) NOT IN (SELECT * FROM dup_campaigns)
UNION ALL
SELECT
    source,
    date,
    campaign,
    brand,
    delivery_label,
    tracking_code,
    country,
    type,
    0 delivered,
    0 opened,
    0 unique_opened,
    0 clicked,
    0 unique_click,
    revenue,
    revenue_local,
    orders,
    unsubscribe,
    visits
FROM
    all_data
WHERE
    CONCAT(date,campaign) IN (SELECT * FROM dup_campaigns)
UNION ALL
SELECT
    source,
    date,
    campaign,
    brand,
    delivery_label,
    tracking_code,
    country,
    type,
    delivered,
    opened,
    unique_opened,
    clicked,
    unique_click,
    0 revenue,
    0 revenue_local,
    0 orders,
    0 unsubscribe,
    0 visits
FROM
    all_data
WHERE
    CONCAT(date,campaign) IN (SELECT * FROM dup_campaigns) AND source = 'OLD'
)
)


SELECT
upper(CONCAT(brand, '-', country)) AS ecommerce
,date
,campaign
,brand
,delivery_label
,tracking_code
,country
,type
,delivered
,opened
,unique_opened
,clicked
,unique_click
,revenue
,revenue_local
,orders
,unsubscribe
,visits
FROM
  all_data_final

UNION ALL --NO COOKIES

SELECT
upper(CONCAT(brand, '-', country)) AS ecommerce
,first_day_of_month as date
,campaign
,brand
,'' as delivery_label
,'' as tracking_code
,country
,'EMAIL' AS type
,0 as delivered
,0 as opened
,0 as unique_opened
,0 as clicked
,0 as unique_click
,revenue
,revenue_local
,0 as orders
,0 as unsubscribe
,0 as visits
FROM `dataset.repository.conversion_no_cookies`
