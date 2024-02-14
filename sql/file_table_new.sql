CREATE TABLE IF NOT EXISTS file_table (
  name    varchar NOT NULL,
  dir     varchar NOT NULL,
  source  varchar NOT NULL,
  status  varchar,
  ranking int NOT NULL,
  created DATETIME DEFAULT NULL,
  added   DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT unique_file PRIMARY KEY (name, dir)
)
