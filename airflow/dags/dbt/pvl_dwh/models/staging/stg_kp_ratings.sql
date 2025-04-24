select
    md5(cast(id as string))                                 as id,
    cast(id as string)                                      as kp_id,
    'kp'                                                    as source,
    nameeng                                                 as title_english,
    namerus                                                 as title_russian,
    cast(rating as int64)                                   as rating,
    cast(parse_datetime('%d.%m.%Y, %H:%M', date) as date)   as rating_date,
    cast(year as int64)                                   as release_year,
    link                                                    as link
from {{ source('film_ratings', 'kp') }}
