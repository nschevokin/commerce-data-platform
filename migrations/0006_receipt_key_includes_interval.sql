ALTER TABLE control.file_receipts DROP CONSTRAINT file_receipts_pkey;
ALTER TABLE control.file_receipts
  ADD PRIMARY KEY (replay_version, source_name, interval_start, file_sha256);
