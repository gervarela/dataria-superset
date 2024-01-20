SELECT 
  
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  Airport_fee,

  {% if filter_values('comparison_type')|length > 0 %}
    '{{ filter_values("comparison_type")[0] }}' as comparison_type,
  {% else %}
  {% set comparison_type = 'none' %}
    'none' as comparison_type,
  {% endif %}

  {% if from_dttm is not none and to_dttm is not none %}
    if (tpep_pickup_datetime >= toDate('{{ from_dttm }}') AND tpep_pickup_datetime < toDate('{{ to_dttm }}'), 'current', 'previous') as period
  {% else %}
    'current' as period
  {% endif %},
  {% if filter_values('comparison_type')|length > 0 and filter_values('comparison_type', remove_filter=True)[0] == 'previous-year' %}
    if(period = 'previous', addWeeks(tpep_pickup_datetime, 52), tpep_pickup_datetime) as filter_date
  {% elif filter_values('comparison_type')|length > 0 and filter_values('comparison_type', remove_filter=True)[0] == 'previous-period' %}
    if(period = 'previous', addDays(tpep_pickup_datetime, date_diff(day, toDate('{{ from_dttm }}'), toDate('{{ to_dttm }}'))), tpep_pickup_datetime) as filter_date
  {% elif filter_values('comparison_type')|length > 0 and  filter_values('comparison_type', remove_filter=True)[0] == 'previous-month' %}
    if(period = 'previous', addDays(tpep_pickup_datetime, date_diff(day, toDate('{{ from_dttm }}'), toDate('{{ to_dttm }}'))), tpep_pickup_datetime) as filter_date
  {% else %}
    tpep_pickup_datetime as filter_date
  {% endif %}

FROM nyc_yellow_taxis 

WHERE
  (
    {% if from_dttm is not none %}
      tpep_pickup_datetime >= toDate('{{ from_dttm }}') AND 
    {% endif %}
    {% if to_dttm is not none %}
      tpep_pickup_datetime < toDate('{{ to_dttm }}') AND
    {% endif %}
    true
  )
  OR
  (
    {% if from_dttm is not none and comparison_type != 'none' %}
      {% if filter_values('comparison_type')|length > 0 and filter_values('comparison_type', remove_filter=True)[0] == 'previous-year' %}
        tpep_pickup_datetime >= subtractWeeks(toDate('{{ from_dttm }}'), 52) AND
      {% elif filter_values('comparison_type')|length > 0 and filter_values('comparison_type', remove_filter=True)[0] == 'previous-period' %}
        tpep_pickup_datetime >= subtractDays(toDate('{{ from_dttm }}'), date_diff(day, toDate('{{ from_dttm }}'), toDate('{{ to_dttm }}'))) AND
      {% elif filter_values('comparison_type')|length > 0 and filter_values('comparison_type', remove_filter=True)[0] == 'previous-month' %}
        tpep_pickup_datetime >= toDate(toStartOfMonth(subtractMonths(toDate('{{ from_dttm }}'), 1))) AND
      {% endif %}
    {% endif %}
    
    {% if to_dttm is not none %}
      {% if filter_values('comparison_type')|length > 0 and filter_values('comparison_type', remove_filter=True)[0] == 'previous-year' %}
        tpep_pickup_datetime < subtractWeeks(toDate('{{ to_dttm }}'), 52) AND
      {% elif filter_values('comparison_type')|length > 0 and filter_values('comparison_type', remove_filter=True)[0] == 'previous-period' %}
        tpep_pickup_datetime < subtractDays(toDate('{{ to_dttm }}'), date_diff(day, toDate('{{ from_dttm }}'), toDate('{{ to_dttm }}'))) AND
      {% elif filter_values('comparison_type')|length > 0 and filter_values('comparison_type', remove_filter=True)[0] == 'previous-month' %}
        tpep_pickup_datetime < toDate(toLastDayOfMonth(subtractMonths(toDate('{{ to_dttm }}'), 1))) AND
      {% endif %}
    {% endif %}

    true
  )
