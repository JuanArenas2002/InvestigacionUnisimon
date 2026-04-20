-- Migration v16: Add abstract, page_range, publisher to all source record tables
-- These fields are now part of SourceRecordMixin and StandardRecord.

ALTER TABLE openalex_records        ADD COLUMN IF NOT EXISTS abstract   TEXT;
ALTER TABLE openalex_records        ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);
ALTER TABLE openalex_records        ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);

ALTER TABLE scopus_records          ADD COLUMN IF NOT EXISTS abstract   TEXT;
ALTER TABLE scopus_records          ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);
ALTER TABLE scopus_records          ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);

ALTER TABLE wos_records             ADD COLUMN IF NOT EXISTS abstract   TEXT;
ALTER TABLE wos_records             ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);
ALTER TABLE wos_records             ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);

ALTER TABLE cvlac_records           ADD COLUMN IF NOT EXISTS abstract   TEXT;
ALTER TABLE cvlac_records           ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);
ALTER TABLE cvlac_records           ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);

ALTER TABLE datos_abiertos_records  ADD COLUMN IF NOT EXISTS abstract   TEXT;
ALTER TABLE datos_abiertos_records  ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);
ALTER TABLE datos_abiertos_records  ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);

ALTER TABLE google_scholar_records  ADD COLUMN IF NOT EXISTS abstract   TEXT;
ALTER TABLE google_scholar_records  ADD COLUMN IF NOT EXISTS page_range VARCHAR(100);
ALTER TABLE google_scholar_records  ADD COLUMN IF NOT EXISTS publisher  VARCHAR(500);
