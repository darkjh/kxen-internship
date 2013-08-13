--------------------------------------------------------------------------------
-- script for a bipartite graph projection onto product space
-- input: user product count
-- output: prod1 prod2 support

-- param
--   in: input file path
--   out: output file path
--   supp: minimun support for graph link
--------------------------------------------------------------------------------

raw_1 = LOAD '$in' USING PigStorage('\t') AS (u1:long, s1:long, t:int);
all_1 = FOREACH raw_1 GENERATE u1, s1;
raw_2 = LOAD '$in' USING PigStorage('\t') AS (u2:long, s2:long, t:int);
all_2 = FOREACH raw_2 GENERATE u2, s2;
pairs = FOREACH (JOIN all_1 BY u1, all_2 BY u2 PARALLEL 8) GENERATE s1, s2;
filtered_pairs = FILTER pairs BY s1 < s2;
ap = FOREACH (GROUP filtered_pairs BY (s1, s2)) GENERATE group.s1, group.s2,
	COUNT(filtered_pairs) AS count;
final = FILTER ap BY count >= $supp;

STORE final INTO '$out' USING PigStorage('\t');