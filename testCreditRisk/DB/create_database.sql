DROP DATABASE IF EXISTS TestDB;
CREATE DATABASE TestDB;
DROP TABLE IF EXIST test1;
CREATE TABLE test1(
    date                  DATE,
    lat_symb              VARCHAR(10),
    ru_symb           VARCHAR(10),
    int_symb            INT,
    float_symb           DECIMAL(10,8)

);