with base as (
    select
        *
    from {{ ref('customer_master') }}
),

updates as (
    select
        b.groupkey,
        b.company_code,
        b.lifecycle_stage,
        b.db_created_date,
        c.lifecycle_label
    from {{ source('us_cdp_dev', 'PACE_LASTLIFECYCLESTAGE') }} b
    join {{ source('us_cdp_dev', 'LIFECYCLE') }} c
        on b.lifecycle_stage = c.lifecycle_code
),

final as (
    select
        base.*,
        updates.lifecycle_stage as lastlifecycle_lcs_stage,
        updates.db_created_date as lastlifecycle_lcs_stage_update,
        updates.lifecycle_label as lastlifecycle_lcs_stage_text
    from base
    left join updates
        on base.groupkey = updates.groupkey
        and base.company_code = updates.company_code
)

select * from final