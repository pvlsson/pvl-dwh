select
    md5(cast(const as string))      as rating_id,
    cast(const as string)           as imdb_id,
    'imdb'                          as rating_source,
    `your rating`                   as rating,
    `date rated`                    as rating_date,
    title                           as title_english,
    `original title`                as title_original,
    url                             as link,
    `title type`                    as title_type,
    `imdb rating`                   as imdb_rating,
    runtimemins                     as runtime_mins,
    year                            as release_year,
    genres                          as genres,
    `num votes`                     as votes_num,
    `release date`                  as release_date,
    directors                       as directors
from {{ source('film_ratings', 'imdb_updated') }}