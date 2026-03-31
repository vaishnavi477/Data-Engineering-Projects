with source_data as (

    select
        encounter_id,
        patient_key,
        provider_id,
        admit_date,
        discharge_date,
        visit_type,
        total_cost,
        -- Length of stay in days
        (discharge_date::date - admit_date::date) as length_of_stay
    from {{ source('deidentified', 'encounters') }}

)

select * from source_data
