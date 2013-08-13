-- create raw data table
CREATE EXTERNAL TABLE movielens.raw (
user BIGINT,
prod BIGINT,
count INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/port/dataset/ml-10m';

-- projection
CREATE TABLE proj_results
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
AS SELECT r1.prod p1, r2.prod p2, count(*) cc
FROM raw r1 JOIN raw r2 ON r1.user = r2.user
WHERE r1.prod < r2.prod
GROUP BY r1.prod, r2.prod
HAVING count(*) >= 2;


--SELECT prod, FROM raw
--GROUP BY prod
--HAVING count(*) >= 3;