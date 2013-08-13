--------------------------------------------------------------------------------
-- script for 'explain'
--------------------------------------------------------------------------------

-- register and init the udf
REGISTER /home/port/jars/pigudfs.jar;
DEFINE Counter com.kxen.han.pig.Counter('2');

raw_1 = LOAD '/user/hadoop/han/datasets/msd/msd_train_data' USING PigStorage('\t') AS (u1:long, s1:long, t:int);
all_1 = FOREACH raw_1 GENERATE u1, s1;
raw_2 = LOAD '/user/hadoop/han/datasets/msd/msd_train_data' USING PigStorage('\t') AS (u2:long, s2:long, t:int);
all_2 = FOREACH raw_2 GENERATE u2, s2;
pairs = FOREACH (JOIN all_1 BY u1, all_2 BY u2 PARALLEL 8) GENERATE s1, s2;
filtered_pairs = FILTER pairs BY s1 != s2;

gp = FOREACH (GROUP filtered_pairs BY s1) generate group, filtered_pairs.s2;
final = FILTER (FOREACH gp GENERATE group, Counter($1)) BY NOT IsEmpty($1);
STORE final INTO '/user/hadoop/han/output/pig/big_pairs' USING PigStorage('\t');