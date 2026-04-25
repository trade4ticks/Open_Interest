-- Run ONCE on the VPS as a superuser (e.g. postgres) before init_db.py.
-- Assumes the 'portfolio' role already exists from your other projects.
--
--   psql -U postgres -f sql/00_create_database.sql
--
-- To remove the project later:
--   DROP DATABASE open_interest;

CREATE DATABASE open_interest OWNER portfolio;
