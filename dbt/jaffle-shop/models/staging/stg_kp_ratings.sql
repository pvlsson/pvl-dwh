select
    id                          as kp_id,
    name_eng                    as title_english,
    name_rus                    as title_russian,
    rating                      as rating,
    date(rating_datetime)       as rating_date,
    release_year                as release_year,
    link                        as link,
    'kp'                        as source
from {{ source('film_ratings', 'kp_ratings') }}
