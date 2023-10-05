CREATE DATABASE IF NOT EXISTS demo;

SHOW DATABASES;
DESC DATABASE EXTENDED demo;

-- TO check current DB
SELECT current_database()

-- switch to different DB
USE default;
SELECT current_database()

-- To see tables in DB
SHOW TABLES IN default;
