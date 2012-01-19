data = 
	LOAD 'test/hit_data.tsv' 
	USING com.tgam.hadoop.pig.OmnitureTextLoader('visid_high', 'visid_low')
	AS (visid_high:chararray, visid_low:chararray);
	
correct_id = 
	FOREACH data
	GENERATE CONCAT(visid_low, visid_high) AS visid;

grouped = 
	GROUP correct_id
	BY visid;

counted = 
	FOREACH grouped
	GENERATE
		group AS visid,
		COUNT(data) AS total;

ordered = 
	ORDER counted
	BY total DESC;

limited =
	LIMIT ordered 2;

DUMP limited;
STORE limited INTO 'output';