CREATE TABLE IF NOT EXISTS file_table (
  name varchar NOT NULL,
  dir varchar NOT NULL,
  source varchar NOT NULL,
  status varchar NOT NULL,
  created datetime DEFAULT NULL,
  added   datetime DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT unique_file PRIMARY KEY (name, dir)
)
