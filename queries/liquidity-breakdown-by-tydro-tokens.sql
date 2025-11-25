with

pools as (
select
    distinct to_address as pool_address
from
    ink.core.ez_token_transfers a
where 
    origin_to_address = '0x991d5546c4b442b4c5fdc4c8b8b8d131deb24702'
    and origin_from_address != to_address
    and tx_hash in (select distinct tx_hash from ink.core.ez_decoded_event_logs b where a.tx_hash = b.tx_hash and b.event_name = 'IncreaseLiquidity')
),

main as (
select
    symbol,
    contract_address as token_address,
    address as pool_address,
    balance,
    balance_usd
from
    ink.balances.ez_balances_erc20_daily
where
    address in (select pool_address from pools)
    and symbol in ('GHO', 'USDG', 'WETH', 'USD₮0', 'ETH', 'kBTC')
qualify row_number() over (partition by address, contract_address order by block_date desc) = 1
)

select
    symbol,
    sum(balance) as liquidity,
    sum(balance_usd) as liquidity_usd
from
    main
group by 1
