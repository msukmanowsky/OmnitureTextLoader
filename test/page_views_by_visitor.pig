data = 
	LOAD 'test/hit_data.tsv' 
	USING com.tgam.hadoop.pig.OmnitureTextLoader();

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
		COUNT(correct_id) AS total;

ordered = 
	ORDER counted
	BY total DESC;

limited = 
	LIMIT ordered 5;

STORE limited INTO 'output';