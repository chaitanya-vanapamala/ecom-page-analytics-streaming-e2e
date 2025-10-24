
-- Load Time Dimension
SELECT populate_dim_time();

-- Check Time Dimension Count
SELECT COUNT(*) FROM dim_time;


-- Load Date Dimension
SELECT populate_dim_date('2025-01-01', '2030-12-31');


-- Check Date Dimension Count
SELECT COUNT(*) FROM dim_date;


-- Load Locations
SELECT populate_dim_locations();

SELECT COUNT(*) FROM dim_locations;


-- Load dummy users
SELECT populate_dim_users(1, 20);

SELECT COUNT(*) FROM dim_users;

