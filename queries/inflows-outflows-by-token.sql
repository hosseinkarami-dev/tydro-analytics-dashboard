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
    event_name,
    symbol,
    sum(amount) as volume,
    sum(amount_usd) as volume_usd,
    avg(amount) as average_amount,
    avg(amount_usd) as average_amount_usd
from
    main
where
    event_name in ('Supply', 'Withdraw')
    and {condition}
group by 1, 2