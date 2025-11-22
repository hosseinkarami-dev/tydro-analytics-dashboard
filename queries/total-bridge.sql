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
    event_name = 'Borrow'
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
    event_name = 'Borrow'
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
),

bridged_assets as (
    select
        sum(amount_usd) as total_bridged_out
    from
        bridge_activity.defi.ez_bridge_activity
    where
        'ink' = source_chain
        and token_symbol in ('GHO', 'USDG', 'WETH', 'USD₮0', 'USDT', 'ETH')
        and iff('ink' = source_chain, source_address, destination_address) in (select distinct user from main)
	and {condition}
),

borrowed_assets as (
    select
        sum(amount_usd) as total_borrowed_within_ink
    from
        main  -- Using your 'main' CTE to get borrowed assets
    where {condition}
),

-- Final Calculation (Percentage of Borrowed Assets Retained in Ink)
final_data as (
    select
        b.total_borrowed_within_ink,
        br.total_bridged_out,
        (b.total_borrowed_within_ink / (b.total_borrowed_within_ink + br.total_bridged_out)) * 100 as percentage_retained_in_ink
    from
        borrowed_assets b,
        bridged_assets br
)

select
    total_borrowed_within_ink,
    total_bridged_out,
    percentage_retained_in_ink
from final_data
