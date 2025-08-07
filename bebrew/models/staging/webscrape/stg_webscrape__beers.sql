-- stg_webscrape__beers.sql

{{ config(materialized='table') }}

with source as (

    select * from {{ source('webscrape','wiki_be_beers') }}

),

renamed as (
    select
        beer_name,
        -- Safely access the split parts of the 'style_name'
        -- Use TRIM to remove leading/trailing whitespace from the split values
        trim(split(style_name, ',')[safe_offset(0)]) as beer_style_first,
        trim(split(style_name, ',')[safe_offset(1)]) as beer_style_second,
        trim(split(style_name, ',')[safe_offset(2)]) as beer_style_third,
        abv_pct as abv_percentage,
        brewery_name

    from source
)

select * from renamed
where beer_style_first is not null
    and beer_style_first != ''
    and beer_name is not null
    and beer_name != ''
    and brewery_name is not null
    and brewery_name != ''
