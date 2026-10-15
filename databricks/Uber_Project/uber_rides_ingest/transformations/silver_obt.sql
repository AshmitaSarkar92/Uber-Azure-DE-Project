CREATE OR REFRESH STREAMING TABLE uber_cat.silver.silver_obt
AS
SELECT
    stream_rides_stg.ride_id, stream_rides_stg.confirmation_number, stream_rides_stg.passenger_id, stream_rides_stg.driver_id, stream_rides_stg.vehicle_id, stream_rides_stg.pickup_location_id, stream_rides_stg.dropoff_location_id, stream_rides_stg.vehicle_type_id, stream_rides_stg.vehicle_make_id, stream_rides_stg.payment_method_id, stream_rides_stg.ride_status_id, stream_rides_stg.pickup_city_id, stream_rides_stg.dropoff_city_id, stream_rides_stg.cancellation_reason_id, stream_rides_stg.passenger_name, stream_rides_stg.passenger_email, stream_rides_stg.passenger_phone, stream_rides_stg.driver_name, stream_rides_stg.driver_rating, stream_rides_stg.driver_phone, stream_rides_stg.driver_license, stream_rides_stg.vehicle_model, stream_rides_stg.vehicle_color, stream_rides_stg.license_plate, stream_rides_stg.pickup_address, stream_rides_stg.pickup_latitude, stream_rides_stg.pickup_longitude, stream_rides_stg.dropoff_address, stream_rides_stg.dropoff_latitude, stream_rides_stg.dropoff_longitude, stream_rides_stg.distance_miles, stream_rides_stg.duration_minutes, stream_rides_stg.booking_timestamp, stream_rides_stg.pickup_timestamp, stream_rides_stg.dropoff_timestamp, stream_rides_stg.base_fare, stream_rides_stg.distance_fare, stream_rides_stg.time_fare, stream_rides_stg.surge_multiplier, stream_rides_stg.subtotal, stream_rides_stg.tip_amount, stream_rides_stg.total_fare, stream_rides_stg.rating,
    map_vehicle_makes.vehicle_make,
    map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate, map_vehicle_types.per_mile, map_vehicle_types.per_minute,
    map_ride_statuses.ride_status,
    map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth,
    map_cities.city AS pickup_city, map_cities.state, map_cities.region,map_cities.updated_at as city_updated_at,
    map_cancellation_reasons.cancellation_reason
FROM
    STREAM(uber_cat.silver.stream_rides_stg)
    WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES stream_rides_stg
    LEFT JOIN uber_cat.bronze.map_vehicle_makes map_vehicle_makes ON stream_rides_stg.vehicle_make_id = map_vehicle_makes.vehicle_make_id
    LEFT JOIN uber_cat.bronze.map_vehicle_types map_vehicle_types ON stream_rides_stg.vehicle_type_id = map_vehicle_types.vehicle_type_id
    LEFT JOIN uber_cat.bronze.map_ride_statuses map_ride_statuses ON stream_rides_stg.ride_status_id = map_ride_statuses.ride_status_id
    LEFT JOIN uber_cat.bronze.map_payment_methods map_payment_methods ON stream_rides_stg.payment_method_id = map_payment_methods.payment_method_id
    LEFT JOIN uber_cat.bronze.map_cities map_cities ON stream_rides_stg.pickup_city_id = map_cities.city_id
    LEFT JOIN uber_cat.bronze.map_cancellation_reasons map_cancellation_reasons ON stream_rides_stg.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
    