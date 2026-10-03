ALTER TABLE event_media
    ADD COLUMN upload_id VARCHAR(300),
    ADD COLUMN multipart_part_size BIGINT;
