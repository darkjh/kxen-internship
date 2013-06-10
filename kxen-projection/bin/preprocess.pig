--------------------------------------------------------------------------------
-- script for preprocess a dataset
-- input: edge format dataset with arbitrary user and product ids (strings)
-- output: edge format dataset with user and product id in long format

-- param
--   in: input file path
--   out: output file path
--------------------------------------------------------------------------------

-- assume that user and item are the first two columns
raw = LOAD '$in' USING PigStorage('\t') AS (u:chararray, p:chararray);

users = RANK (DISTINCT (FOREACH raw GENERATE u));
user_index = FOREACH users GENERATE $0 AS rank_user, $1 AS user;

items = RANK (DISTINCT (FOREACH raw GENERATE p));
item_index = FOREACH items GENERATE $0 AS rank_item, $1 AS item;

-- join with user_index
tmp = JOIN raw BY u, user_index BY user;
user_joined = FOREACH tmp GENERATE rank_user, p;

-- join with item_index
tmp = JOIN user_joined BY p, item_index BY item;
item_joined = FOREACH tmp GENERATE rank_user, rank_item;

-- store results
STORE item_joined INTO '$out' USING PigStorage('\t');