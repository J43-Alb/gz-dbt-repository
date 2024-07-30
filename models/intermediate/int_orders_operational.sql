-- models/intermediate/int_orders_operational.sql

with orders_margin as (
    select * from {{ ref('int_sales_margin') }}
),

ship_data as (
    select
        orders_id,
        cast(shipping_fee as numeric) as shipping_fee,
        cast(logcost as numeric) as logcost,
        cast(ship_cost as numeric) as ship_cost
    from {{ ref('stg_raw_data__ship') }}
),

operational_margin as (
    select
        om.orders_id,
        om.date_date,
        om.revenue,
        om.quantity,
        om.purchase_cost,
        om.margin,
        (om.margin + sd.shipping_fee - sd.logcost - sd.ship_cost) as operational_margin
    from orders_margin om
    join ship_data sd on om.orders_id = sd.orders_id
)

select * from operational_margin