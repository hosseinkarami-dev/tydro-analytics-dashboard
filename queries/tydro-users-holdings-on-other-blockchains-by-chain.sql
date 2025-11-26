with

tydro_users as (
select
    distinct origin_from_address as user
from
    ink.core.ez_decoded_event_logs
where
    event_name in ('Supply', 'Withdraw', 'Borrow', 'Repay')
    and origin_to_address = '0x2816cf15f6d2a220e789aa011d5ee4eb6c47feba'
    and tx_succeeded
),

main as (
select
    'Optimism' as chain,
    symbol,
    contract_address as token_address,
    address as user,
    balance,
    balance_usd
from
    optimism.balances.ez_balances_erc20_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'Optimism' as chain,
    'ETH' as symbol,
    null,
    address as user,
    balance,
    balance_usd
from
    optimism.balances.ez_balances_native_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'Ethereum' as chain,
    symbol,
    contract_address as token_address,
    address as user,
    balance,
    balance_usd
from
    ethereum.balances.ez_balances_erc20_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'Ethereum' as chain,
    'ETH' as symbol,
    null,
    address as user,
    balance,
    balance_usd
from
    ethereum.balances.ez_balances_native_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'Arbitrum' as chain,
    symbol,
    contract_address as token_address,
    address as user,
    balance,
    balance_usd
from
    arbitrum.balances.ez_balances_erc20_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'Arbitrum' as chain,
    'ETH' as symbol,
    null,
    address as user,
    balance,
    balance_usd
from
    arbitrum.balances.ez_balances_native_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'BSC' as chain,
    symbol,
    contract_address as token_address,
    address as user,
    balance,
    balance_usd
from
    bsc.balances.ez_balances_erc20_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'BSC' as chain,
    'BNB' as symbol,
    null,
    address as user,
    balance,
    balance_usd
from
    bsc.balances.ez_balances_native_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'Base' as chain,
    symbol,
    contract_address as token_address,
    address as user,
    balance,
    balance_usd
from
    base.balances.ez_balances_erc20_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'Base' as chain,
    'ETH' as symbol,
    null,
    address as user,
    balance,
    balance_usd
from
    base.balances.ez_balances_native_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'Avalanche' as chain,
    symbol,
    contract_address as token_address,
    address as user,
    balance,
    balance_usd
from
    avalanche.balances.ez_balances_erc20_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1

union all

select
    'Avalanche' as chain,
    'ETH' as symbol,
    null,
    address as user,
    balance,
    balance_usd
from
    avalanche.balances.ez_balances_native_daily
where
    address in (select user from tydro_users)
    and balance_usd > 0
qualify row_number() over (partition by address, symbol order by block_date desc) = 1
)

select
    chain,
    sum(balance_usd) as balance_usd
from
    main
    group by 1
order by 2 desc