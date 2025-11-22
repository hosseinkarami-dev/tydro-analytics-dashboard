with tydro_events as (
    select
        origin_from_address as user,
        min(block_timestamp) as first_tydro_time
    from INK.CORE.EZ_DECODED_EVENT_LOGS
    where
        event_name in ('Supply', 'Withdraw', 'Borrow', 'Repay')
        and origin_to_address in (
            '0x2816cf15f6d2a220e789aa011d5ee4eb6c47feba',
            '0xde090efcd6ef4b86792e2d84e55a5fa8d49d25d2'
        )
        and tx_succeeded
        and block_timestamp::date >= '2025-10-13'
	and {condition}
    group by 1

    union all

    select
        origin_from_address as user,
        max(block_timestamp) as first_tydro_time
    from INK.CORE.EZ_DECODED_EVENT_LOGS
    where
        event_name in ('Supply', 'Withdraw', 'Borrow', 'Repay')
        and origin_to_address in (
            '0x2816cf15f6d2a220e789aa011d5ee4eb6c47feba',
            '0xde090efcd6ef4b86792e2d84e55a5fa8d49d25d2'
        )
        and tx_succeeded
        and block_timestamp::date >= '2025-10-13'
	and {condition}
    group by 1
),

user_events as (
    select
        e.origin_from_address as user,
        e.event_name,
        e.block_timestamp
    from INK.CORE.EZ_DECODED_EVENT_LOGS e
    join tydro_events t
        on e.origin_from_address = t.user
       -- and e.block_timestamp >= t.first_tydro_time - interval '7 days'
       -- and e.block_timestamp <= t.first_tydro_time + interval '7 days'
    where
        e.tx_succeeded
        and event_name != 'Approval'
),

before_actions as (
    select
        user,
        event_name,
        'Before' as action_type
    from user_events u
    join tydro_events t using(user)
    where u.block_timestamp < t.first_tydro_time
    qualify row_number() over (partition by user order by block_timestamp desc) = 1
),

after_actions as (
    select
        user,
        event_name,
        'After' as action_type
    from user_events u
    join tydro_events t using(user)
    where u.block_timestamp > t.first_tydro_time
    qualify row_number() over (partition by user order by block_timestamp) = 1
)

select
    action_type,
    event_name,
    count(distinct user) as users
from (
    select * from before_actions
    union all
    select * from after_actions
)
group by 1,2
qualify row_number() over (partition by action_type order by users desc) <= 10
order by 1, users desc