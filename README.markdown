# OmnitureTextLoader

## what is it
An Apache Pig UDF (custom Loader) which allows the reading and parsing of raw Omniture data files (hit_data.tsv).

Looking to perform analysis with Hive instead of Pig? You can simply use the underlying InputFormat directly see [OmnitureDataFileInputFormat](https://github.com/msukmanowsky/OmnitureDataFileInputFormat).

## usage in pig
To use `OmnitureTextLoader` in a Pig script, you will first have to register both the [InputFormat](https://github.com/msukmanowsky/OmnitureDataFileInputFormat) well as the UDF.

    REGISTER 'path/to/jar/OmnitureDataInputFileFormat.jar';
    REGISTER 'path/to/jar/OmnitureTextLoader.jar';

Once the JARs are registered, you're able to use the custom loader:

    A = LOAD '/path/to/data/hit_data.tsv' USING com.tgam.hadoop.pig.OmnitureTextLoader();
    
Notice that you do not have to specify a schema for the file because `OmnitureTextLoader` already understands the schema of hit_data.tsv files (this is taken care of via it's implementation of `LoadMetadata` - thank you for the heads up [Dmitriy Ryaboy](https://twitter.com/#!/squarecog)!).  You'll have to refer to Omniture's documentation as to specific field names (see ClickStreamData.pdf).

Here's a small sample script to count the number of hits (not necessarily page views) by visitor:

    REGISTER 'path/to/jar/OmnitureDataInputFileFormat.jar';
    REGISTER 'path/to/jar/OmnitureTextLoader.jar';

    data = LOAD 'hit_data.tsv' USING com.tgam.hadoop.pig.OmnitureTextLoader();
    
    correct_id = 
      FOREACH data
      GENERATE CONCAT(visid_low, visid_high) AS visid;
    
    grouped = 
      GROUP correct_id BY visid;
    
    counted = 
      FOREACH grouped
      GENERATE group AS visid, COUNT(correct_id) AS total;
    
    ordered = 
      ORDER counted BY total DESC;
    
    STORE ordered INTO 'output';