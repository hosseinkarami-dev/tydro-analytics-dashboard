with

main as (
select
    'Inflow' as direction,
    token_symbol as symbol,
    count(distinct tx_hash) as transactions,
    sum(amount_usd) as volume_usd,
    avg(amount_usd) as average_amount_usd
from
    bridge_activity.defi.ez_bridge_activity
where
    'ink' = destination_chain
    and token_symbol in ('GHO', 'USDG', 'WETH', 'USD₮0', 'USDT', 'ETH', 'kBTC')
group by 1, 2

union all

select
    'Outflow' as direction,
    token_symbol as symbol,
    count(distinct tx_hash) as transactions,
    sum(amount_usd) as volume_usd,
    avg(amount_usd) as average_amount_usd
from
    bridge_activity.defi.ez_bridge_activity
where
    'ink' = source_chain
    and token_symbol in ('GHO', 'USDG', 'WETH', 'USD₮0', 'USDT', 'ETH', 'kBTC')
group by 1, 2
)

select * from main where {condition} order by direction, symbol