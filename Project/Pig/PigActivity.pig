-- Episode4
-- Load input file
inputFile = LOAD 'hdfs:///user/nandini/episodeIV_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
-- Group By Data using name
groupByName = GROUP inputFile By name;
-- Generate Result
names = FOREACH groupByName GENERATE $0,COUNT($1) as nameDialogue;
nameCount = ORDER names BY nameDialogue DESC;
-- Store Result to HDFS
STORE nameCount  INTO 'hdfs:///user/nandini/episodeIVActivityOutput';

-- Episode5
-- Load input file
inputFile = LOAD 'hdfs:///user/nandini/episodeV_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
-- Group By Data using name
groupByName = GROUP inputFile By name;
-- Generate Result
names = FOREACH groupByName GENERATE $0,COUNT($1) as nameDialogue;
nameCount = ORDER names BY nameDialogue DESC;
-- Store Result to HDFS
STORE nameCount  INTO 'hdfs:///user/nandini/episodeVActivityOutput';


-- Episode6
-- Load input file
inputFile = LOAD 'hdfs:///user/nandini/episodeVI_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
-- Group By Data using name
groupByName = GROUP inputFile By name;
-- Generate Result
names = FOREACH groupByName GENERATE $0,COUNT($1) as nameDialogue;
nameCount = ORDER names BY nameDialogue DESC;
-- Store Result to HDFS
STORE nameCount  INTO 'hdfs:///user/nandini/episodeVIActivityOutput';