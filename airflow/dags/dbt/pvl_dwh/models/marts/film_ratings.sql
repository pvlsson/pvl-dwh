with
    merge_with_kp as (
        select
            id,
            source,
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
        from {{ ref('stg_imdb_ratings') }}
        union all
        select
            id,
            source,
            rating,
            rating_date,
            title_english,
            title_russian,
            null as title_original,
            link,
            null as title_type,
            null as imdb_rating,
            null as runtime_mins,
            release_year,
            null as genres,
            null as votes_num,
            null as release_date,
            null as directors
        from {{ ref('stg_kp_ratings') }}
    )

select *
from merge_with_kp
order by id, rating_date