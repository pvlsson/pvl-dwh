select
    md5(cast(const as string))  as id,
    cast(const as string)       as imdb_id,
    'imdb'                      as source,
    your_rating                 as rating,
    date(date_rated)            as rating_date,
    title                       as title_english,
    original_title              as title_original,
    url                         as link,
    title_type                  as title_type,
    imdb_rating                 as imdb_rating,
    runtime__mins_              as runtime_mins,
    year                        as release_year,
    genres                      as genres,
    num_votes                   as votes_num,
    date(release_date)          as release_date,
    directors                   as directors
from {{ source('film_ratings', 'imdb') }}
