-- stg_webscrape__beer_ratings.sql

{{ config(materialized='table') }}

with source_data as (

    select 'Zenne Y Frontera' as beer_name, 428 as ratings, 4.7 as average_rating
    union all
    select 'Fou Foune' as beer_name, 3258 as ratings, 4.65 as average_rating
    union all
    select 'Lou Pepe' as beer_name, 1897 as ratings, 4.62 as average_rating
    union all
    select 'Trappist Westvleteren Twaalf' as beer_name, 6884 as ratings, 4.61 as average_rating
    union all
    select 'Oude Geuze Vintage 3 Fonteinen' as beer_name, 896 as ratings, 4.58 as average_rating
    union all
    select 'Saint-Lamvinus' as beer_name, 2330 as ratings, 4.54 as average_rating
    union all
    select 'Oude Franboise 3 Fonteinen' as beer_name, 903 as ratings, 4.52 as average_rating
    union all
    select 'Rosé de Gambrinus' as beer_name, 900 as ratings, 4.52 as average_rating
    union all
    select 'Rochefort 10' as beer_name, 10058 as ratings, 4.5 as average_rating
    union all
    select 'Oude Geuze 3 Fonteinen' as beer_name, 1215 as ratings, 4.5 as average_rating
    union all
    select 'Crabbelaer HOMMAGE' as beer_name, 1190 as ratings, 4.5 as average_rating
    union all
    select 'Oude Geuze Cuvée Armand & Gaston' as beer_name, 780 as ratings, 4.49 as average_rating
    union all
    select 'St. Bernardus Abt 12' as beer_name, 10421 as ratings, 4.47 as average_rating
    union all
    select 'Westvleteren Acht' as beer_name, 2414 as ratings, 4.47 as average_rating
    union all
    select 'Schaarbeekse Oude Kriek 3 Fonteinen' as beer_name, 967 as ratings, 4.47 as average_rating
    union all
    select 'Rodenbach Caractère Rouge' as beer_name, 1609 as ratings, 4.46 as average_rating
    union all
    select 'Geuze Cantillon 100% Lambic Bio' as beer_name, 4007 as ratings, 4.45 as average_rating
    union all
    select 'De Garre Tripel' as beer_name, 469 as ratings, 4.4 as average_rating
    union all
    select "Oude Quetsche Tilquin à l'ancienne" as beer_name, 1120 as ratings, 4.38 as average_rating
    union all
    select 'Kriek Cantillon 100% Lambic' as beer_name, 2639 as ratings, 4.37 as average_rating
    union all
    select 'Rochefort 8' as beer_name, 5747 as ratings, 4.33 as average_rating
    union all
    select 'Pannepot' as beer_name, 2457 as ratings, 4.33 as average_rating
    union all
    select 'Cuvée des Jacobins' as beer_name, 2928 as ratings, 4.32 as average_rating
    union all
    select 'Abt 12 Oak Aged' as beer_name, 249 as ratings, 4.35 as average_rating
    union all
    select 'Gouden Carolus Cuvée van de Keizer Imperial Dark' as beer_name, 2017 as ratings, 4.31 as average_rating
    union all
    select 'Chimay Blauw' as beer_name, 8437 as ratings, 4.3 as average_rating
    union all
    select 'Westmalle Tripel' as beer_name, 5037 as ratings, 4.3 as average_rating
    union all
    select 'Tripel Karmeliet' as beer_name, 4619 as ratings, 4.29 as average_rating
)

select * from source_data