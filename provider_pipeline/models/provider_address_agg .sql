select 
    p.id as provider_id,
    COALESCE(
        ARRAY_AGG(
            struct_pack(
                a.id, 
                a.street, 
                a."rank"
            )
        ) FILTER (where a.id is not null),
        []
    ) as addresses
from {{ ref('providers') }} p
left join {{ ref('addresses') }} a
    on p.id = a.id
group by p.id