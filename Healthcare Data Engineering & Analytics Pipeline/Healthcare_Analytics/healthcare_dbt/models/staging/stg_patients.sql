with source_data as (

    select
        patient_key,
        age,
        gender,
        created_at,
        -- bucket age into groups
        case
            when age < 18 then '0-17'
            when age between 18 and 34 then '18-34'
            when age between 35 and 49 then '35-49'
            when age between 50 and 64 then '50-64'
            when age >= 65 then '65+'
            else 'unknown'
        end as age_group
    from {{ source('deidentified', 'patients') }}

)

select * from source_data
