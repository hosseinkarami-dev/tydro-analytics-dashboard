with

pricet as (
select
    hour::date as date,
    token_address,
    avg(price) as token_price_usd
from
    INK.PRICE.EZ_PRICES_HOURLY
where
    token_address != '0x73e0c0d45e048d25fc26fa3159b0aa04bfa4db98'
group by 1, 2

union all

select
    hour::date as date,
    '0x73e0c0d45e048d25fc26fa3159b0aa04bfa4db98',
    avg(price) as token_price_usd
from
    CROSSCHAIN.price.ez_prices_hourly
where
    token_address = '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'
    and blockchain = 'ethereum'
group by 1, 2
),

tbl as (
select
    tx_hash,
    block_timestamp,
    origin_from_address as user,
    decoded_log:amount / pow(10, decimals) as amount,
    a.decoded_log:reserve::string as token_address,
    symbol,
    event_name
from
    INK.CORE.EZ_DECODED_EVENT_LOGS a
left join
    INK.CORE.DIM_CONTRACTS b on b.address = a.decoded_log:reserve
where
    event_name in ('Supply', 'Withdraw', 'Borrow', 'Repay')
    and origin_to_address = '0x2816cf15f6d2a220e789aa011d5ee4eb6c47feba'
    and tx_succeeded

union all

select
    tx_hash,
    block_timestamp,
    origin_from_address as user,
    decoded_log:amount / 1e18 as amount,
    '0x4200000000000000000000000000000000000006' as token_address,
    'ETH' as symbol,
    event_name
from
    INK.CORE.EZ_DECODED_EVENT_LOGS
where
    event_name in ('Supply', 'Withdraw', 'Borrow', 'Repay')
    and origin_to_address = '0xde090efcd6ef4b86792e2d84e55a5fa8d49d25d2'
    and tx_succeeded
),

main as (
select
    tbl.*,
    amount * token_price_usd as amount_usd
from
    tbl
left join
    pricet on block_timestamp::date = date and tbl.token_address = pricet.token_address
)

select
    case 
        when amount_usd < 1000 then '<1K'
        when amount_usd >= 1000 and amount_usd < 25000 then '1–25K'
        when amount_usd >= 25000 and amount_usd < 100000 then '25–100K'
        when amount_usd >= 100000 and amount_usd < 1000000 then '100K–1M+'
        when amount_usd >= 1000000 then '1M+'
    end as deposit_size_range,
    count(*) as deposit_count,
    sum(amount_usd) as total_deposit_usd,
    min(amount_usd) as min_amount_usd,
    max(amount_usd) as max_amount_usd
from
    main
where
    event_name = 'Supply'
    and {condition}
group by
    deposit_size_range
order by
    deposit_size_range