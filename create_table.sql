CREATE TABLE IF NOT EXISTS adds_success_history (
    datetime timestamp without time zone,
    clicks_count bigint,
    impressions_count bigint,
    audit_loaded_datetime timestamp without time zone default NOW()
);