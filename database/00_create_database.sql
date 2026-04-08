-- ============================================================================
-- NatureRisk - Schema setup in the default 'postgres' database
-- ============================================================================
-- All tables are created under the 'nature_risk' schema.
-- Run this as the postgres superuser:
--   psql -U postgres -f database/00_create_database.sql
-- ============================================================================

-- Create the schema
CREATE SCHEMA IF NOT EXISTS nature_risk;

-- Optionally create an application user
-- CREATE USER naturerisk_app WITH PASSWORD 'naturerisk_secure_password';
-- GRANT USAGE ON SCHEMA nature_risk TO naturerisk_app;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA nature_risk TO naturerisk_app;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA nature_risk GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO naturerisk_app;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA nature_risk GRANT USAGE, SELECT ON SEQUENCES TO naturerisk_app;
