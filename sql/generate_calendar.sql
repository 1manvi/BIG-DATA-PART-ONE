INSERT INTO calendar (
    date,
    weekday_name,
    is_weekend,
    month
)
SELECT
    d::date,
    TO_CHAR(d, 'FMDay'),
    EXTRACT(ISODOW FROM d) IN (6, 7),
    EXTRACT(MONTH FROM d)::INTEGER
FROM generate_series(
    '2026-09-01'::date,
    '2026-09-30'::date,
    '1 day'::interval
) AS d;