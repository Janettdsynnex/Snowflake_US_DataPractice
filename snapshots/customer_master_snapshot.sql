{% snapshot customer_master_snapshot %}

{{
    config(
        alias = 'customer_master_snapshot',
        tags = ["customer_master_snapshot"],
        unique_key = 'CUSTOMER_MASTER_KEY',
        strategy = 'timestamp',
        updated_at='UPDATE_DATE_UTC',
        target_schema = 'CORE'

    )
}}

select * from {{ ref('customer_master') }}

{% endsnapshot %}



