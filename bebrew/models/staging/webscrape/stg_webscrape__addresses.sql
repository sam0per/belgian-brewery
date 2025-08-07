-- stg_webscrape__addresses.sql

{{ config(materialized='table') }}

with source as (

    select brewery_name, municipality, postcode, province, latitude, longitude
    from {{ source('webscrape','wiki_be_brewery_addresses') }}

)

select * from source
where municipality is not null
    and municipality != ''
    and brewery_name is not null
    and brewery_name != ''
