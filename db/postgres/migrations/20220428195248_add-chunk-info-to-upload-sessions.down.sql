-- Add down migration script here
ALTER TABLE upload_sessions
DROP COLUMN chunk_info;
