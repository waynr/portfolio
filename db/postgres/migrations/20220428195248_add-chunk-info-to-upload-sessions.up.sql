-- Add up migration script here
ALTER TABLE upload_sessions
ADD COLUMN chunk_info JSONB NOT NULL;
