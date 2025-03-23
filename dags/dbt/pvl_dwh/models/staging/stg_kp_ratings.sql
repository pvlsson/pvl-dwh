select
    md5(cast(id as varchar))    as id,
    id                          as kp_id,
    'kp'                        as source,
    name_eng                    as title_english,
    name_rus                    as title_russian,
    rating                      as rating,
    date(rating_datetime)       as rating_date,
    release_year                as release_year,
    link                        as link
from {{ source('film_ratings', 'kp_ratings') }}
