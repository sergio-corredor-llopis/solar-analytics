with source as (
    select * from {{ source('solar_raw', 'raw_solar_readings') }}
),

renamed as (
    select
        -- identifiers
        "Fecha" as fecha_raw,
        "Hora"  as hora_raw,

        -- we'll cast timestamp properly in a later iteration
        "Timestamp" as timestamp_raw

    from source
)

select * from renamed