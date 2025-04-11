with split_degree_in_provider as (
    select
        p.id as provider_id,
        unnest(split(p.degrees, ',')) as degree
    from {{ ref('providers') }} p
),

matching_degree as (
    select
        sp.provider_id,
        dt.ptui,
        dt.rank
    from split_degree_in_provider sp
    join {{ ref('degree_types') }} dt
        on sp.degree = dt.degree
),

ranked as (
    select
        *,
        row_number() over (PARTITION BY provider_id order by rank ASC) as rn 
    from matching_degree
)
select 
    provider_id,
    ptui
from ranked
where rn = 1