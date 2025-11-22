with

from_cex as (
select
    from_address,
    sum(amount_usd) as volume_usd
from
    ink.core.ez_native_transfers
where
    from_address in (select distinct address from ink.core.dim_labels where label_type = 'cex')
group by 1
    
union all

select
    from_address,
    sum(amount_usd) as volume_usd
from
    ink.core.ez_token_transfers
where
    from_address in (select distinct address from ink.core.dim_labels where label_type = 'cex')
group by 1
)

select
    label,
    sum(volume_usd) as volume_usd
from
    from_cex
join
    ink.core.dim_labels on from_address = address
where
    label_type = 'cex'
    and {condition}
group by 1
