-- Check that all required tables exist
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

-- Check generated calendar row count
SELECT COUNT(*) AS calendar_rows
FROM calendar;

-- Inspect generated calendar
SELECT *
FROM calendar
ORDER BY date;