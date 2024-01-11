with 

source as (

    select * from {{ source('gohighlevel', 'contacts') }}

),

renamed as (

    select
        id as contact_id,
        tags
    from source

),

unnested_tags as(

    select
        contact_id,
        string_agg(tag) as tags,
    -- tags,
    -- json_extract_scalar(tag, "$.id") as tags_id,
    -- json_extract_scalar(tag, "$.value") as tags_value
    from renamed,
    UNNEST(tags) AS tag
    group by 1
    -- WHERE
    --     tag IS NOT NULL AND tag != '' AND json_extract_scalar(tag, "$.id") IS NOT NULL AND json_extract_scalar(tag, "$.value") IS NOT NULL   
)

select * from unnested_tags

