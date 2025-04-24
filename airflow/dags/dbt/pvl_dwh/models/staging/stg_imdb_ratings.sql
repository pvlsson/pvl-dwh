select
    md5(cast(const as string))      as id,
    cast(const as string)           as imdb_id,
    'imdb'                          as source,
    cast(your_rating as int64)      as rating,
    date(date_rated)                as rating_date,
    title                           as title_english,
    original_title                  as title_original,
    url                             as link,
    title_type                      as title_type,
    cast(imdb_rating as float64)    as imdb_rating,
    cast(runtime__mins_ as int64)   as runtime_mins,
    cast(year as int64)           as release_year,
    genres                          as genres,
    cast(num_votes as int64)        as votes_num,
    date(release_date)              as release_date,
    directors                       as directors
from {{ source('film_ratings', 'imdb') }}
