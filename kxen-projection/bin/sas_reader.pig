--------------------------------------------------------------------------------
-- script for reading a SAS format file then extract the bipartite graph
-- input: SAS format file
-- output: edge format dataset with string user and product ids

-- param
--   in: input file path
--   out: output file path
--   userStart: start position of user id
--   userEnd: length of user id field
--   prodStart: start position of prod id
--   prodEnd: length of prod id field
--------------------------------------------------------------------------------

raw = LOAD '$in' USING PigStorage() AS (line:chararray);
links = FOREACH raw GENERATE
      TRIM(SUBSTRING(line, 0, 20)),  -- user id
      TRIM(SUBSTRING(line, 29, 44));  -- prod id
uniq_links = DISTINCT links;
STORE uniq_links INTO '$out' USING PigStorage('\t');