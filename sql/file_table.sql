CREATE TABLE IF NOT EXISTS file_table (
  name varchar UNIQUE,
  path varchar,
  source varchar,
  status varchar,
  date DATETIME DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT unique_file PRIMARY KEY (name, path)
)
