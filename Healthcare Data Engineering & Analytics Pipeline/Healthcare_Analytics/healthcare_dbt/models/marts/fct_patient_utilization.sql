with encounter_agg as (

    select
        patient_key,
        count(encounter_id) as visit_count,
        avg(total_cost) as avg_cost,
        sum(total_cost) as total_cost,
        max(total_cost) as max_cost,
        min(total_cost) as min_cost,
        avg(length_of_stay) as avg_los
    from {{ ref('stg_encounters') }}
    where total_cost >= 0  -- data quality safeguard
    group by patient_key

),

risk_classification as (

    select
        patient_key,
        visit_count,
        avg_cost,
        total_cost,
        max_cost,
        min_cost,
        avg_los,

        case
            when visit_count > 10 or avg_cost > 8000 or avg_los > 7 then 'high_risk'
            when visit_count between 5 and 10 or avg_cost between 4000 and 8000 then 'medium_risk'
            else 'low_risk'
        end as risk_segment

    from encounter_agg

)

select * from risk_classification
