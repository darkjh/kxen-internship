--------------------------------------------------------------------------------
-- script for a bipartite graph projection onto product space
-- input: user product count
-- output: prod1 prod2 support

-- param
--   in: input file path
--   out: output file path
--   supp: minimun support for graph link
--------------------------------------------------------------------------------

-- filter out infrequent items
triples = LOAD '$in' USING PigStorage('\t') AS (u:long, p:long, t:int);
raw_pairs = FOREACH triples GENERATE u, p;
products = FOREACH triples GENERATE p;
freqs = FILTER (FOREACH (GROUP products by p)
    GENERATE group, COUNT(products) AS count) BY count >= $supp;

-- no IN operator in Pig, so do this filtering by using join
pairs_1 = FOREACH (JOIN freqs BY group, raw_pairs BY p)
    GENERATE u AS u1, p AS p1;
pairs_2 = FOREACH pairs_1 GENERATE u1 AS u2, p1 AS p2; -- clear ambiguity, must do

proj_pairs = FOREACH (JOIN pairs_1 BY u1, pairs_2 BY u2 PARALLEL 8) GENERATE p1, p2;
filtered_pairs = FILTER proj_pairs BY p1 < p2;
ap = FOREACH (GROUP filtered_pairs BY (p1, p2)) GENERATE group.p1, group.p2,
	COUNT(filtered_pairs) AS count;
final = FILTER ap BY count >= $supp;

STORE final INTO '$out' USING PigStorage('\t');