-- stg_webscrape__municipalities_population.sql

{{ config(materialized='table') }}

with source_data as (

    select 'Antwerp' as municipality_name, 562000 as population_count, 'Antwerp' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Ghent' as municipality_name, 273000 as population_count, 'East Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Charleroi' as municipality_name, 206000 as population_count, 'Hainaut' as province_name, 'Wallonia' as region_name, '2025-08-01' as last_updated
    union all
    select 'Brussels' as municipality_name, 198000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Liège' as municipality_name, 197000 as population_count, 'Liège' as province_name, 'Wallonia' as region_name, '2025-08-01' as last_updated
    union all
    select 'Schaerbeek' as municipality_name, 130000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Anderlecht' as municipality_name, 129000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Bruges' as municipality_name, 120000 as population_count, 'West Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Namur' as municipality_name, 115000 as population_count, 'Namur' as province_name, 'Wallonia' as region_name, '2025-08-01' as last_updated
    union all
    select 'Leuven' as municipality_name, 105000 as population_count, 'Flemish Brabant' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Molenbeek-Saint-Jean' as municipality_name, 99000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Mons' as municipality_name, 97000 as population_count, 'Hainaut' as province_name, 'Wallonia' as region_name, '2025-08-01' as last_updated
    union all
    select 'Aalst' as municipality_name, 92000 as population_count, 'East Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Hasselt' as municipality_name, 90000 as population_count, 'Limburg' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Ixelles' as municipality_name, 97000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Mechelen' as municipality_name, 90000 as population_count, 'Antwerp' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Beveren-Kruibeke-Zwijndrecht' as municipality_name, 88000 as population_count, 'East Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Uccle' as municipality_name, 87000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Sint-Niklaas' as municipality_name, 83000 as population_count, 'East Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'La Louvière' as municipality_name, 82000 as population_count, 'Hainaut' as province_name, 'Wallonia' as region_name, '2025-08-01' as last_updated
    union all
    select 'Kortrijk' as municipality_name, 81000 as population_count, 'West Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Ostend' as municipality_name, 73000 as population_count, 'West Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Tournai' as municipality_name, 69000 as population_count, 'Hainaut' as province_name, 'Wallonia' as region_name, '2025-08-01' as last_updated
    union all
    select 'Genk' as municipality_name, 68000 as population_count, 'Limburg' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Roeselare' as municipality_name, 67000 as population_count, 'West Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Seraing' as municipality_name, 64000 as population_count, 'Liège' as province_name, 'Wallonia' as region_name, '2025-08-01' as last_updated
    union all
    select 'Woluwe-Saint-Lambert' as municipality_name, 61000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Mouscron' as municipality_name, 60000 as population_count, 'Hainaut' as province_name, 'Wallonia' as region_name, '2025-08-01' as last_updated
    union all
    select 'Forest' as municipality_name, 59000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Verviers' as municipality_name, 56000 as population_count, 'Liège' as province_name, 'Wallonia' as region_name, '2025-08-01' as last_updated
    union all
    select 'Jette' as municipality_name, 54000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Lokeren' as municipality_name, 50000 as population_count, 'East Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Etterbeek' as municipality_name, 49000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Sint-Gilles' as municipality_name, 49000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
    union all
    select 'Beringen' as municipality_name, 49000 as population_count, 'Limburg' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Vilvoorde' as municipality_name, 48000 as population_count, 'Flemish Brabant' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Turnhout' as municipality_name, 48000 as population_count, 'Antwerp' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Dendermonde' as municipality_name, 48000 as population_count, 'East Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Deinze' as municipality_name, 46000 as population_count, 'East Flanders' as province_name, 'Flanders' as region_name, '2025-08-01' as last_updated
    union all
    select 'Evere' as municipality_name, 46000 as population_count, 'Brussels' as province_name, 'Brussels-Capital' as region_name, '2025-08-01' as last_updated
)

select * from source_data