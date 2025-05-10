with
    merge_with_kp as (
        select
            rating_id,
            rating_source,
            rating,
            rating_date,
            title_english,
            null as title_russian,
            title_original,
            link,
            title_type,
            imdb_rating,
            runtime_mins,
            release_year,
            genres,
            votes_num,
            release_date,
            directors
        from {{ ref('stg_imdb_ratings_updated') }}
        union all
        select
            rating_id,
            rating_source,
            rating,
            rating_date,
            title_english,
            title_russian,
            null as title_original,
            link,
            'Movie' as title_type,
            null as imdb_rating,
            null as runtime_mins,
            release_year,
            null as genres,
            null as votes_num,
            null as release_date,
            null as directors
        from {{ ref('stg_kp_ratings') }}
    )

select
    merge_with_kp.*,
    extract(month from rating_date) as rating_month,
    extract(year from rating_date) as rating_year,
    case
        when rating between 0 and 4 then '0-4 (bad)'
        when rating between 5 and 6 then '5-6 (mediocre)'
        when rating between 7 and 8 then '7-8 (good)'
        when rating between 9 and 10 then '9-10 (brilliant)'
    end as rating_group,
    case
        when extract(month from rating_date) in (12, 1, 2) then 'Winter'
        when extract(month from rating_date) in (3, 4, 5) then 'Spring'
        when extract(month from rating_date) in (6, 7, 8) then 'Summer'
        when extract(month from rating_date) in (9, 10, 11) then 'Fall'
    end as rating_season,
    case
        when release_year >= extract(year from rating_date) then "released in the same year"
        else "older film"
    end as film_release_type
from merge_with_kp
order by rating_id, rating_date