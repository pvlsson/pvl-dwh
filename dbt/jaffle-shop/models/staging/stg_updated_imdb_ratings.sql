select
    const                       as imdb_id,
    rating                      as rating,
    date(rating_date)           as rating_date,
    name_eng                    as title_english,
    original_name_eng           as title_original,
    link                        as link,
    title_type                  as title_type,
    imdb_rating                 as imdb_rating,
    runtime                     as runtime,
    release_year                as release_year,
    genres                      as genres,
    votes_num                   as votes_num,
    date(release_date)          as release_date
    directors                   as directors,
    'imdb'                      as source
from {{ source('film_ratings', 'updated_imdb_ratings') }}
