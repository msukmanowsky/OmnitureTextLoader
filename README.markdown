# OmnitureTextLoader

## what is it
A Pig UDF which allows the reading and parsing of raw Omniture data files (hit_data.tsv).

## usage in pig
To use `OmnitureTextLoader` in a Pig script, you will first have to register both the [InputFormat](https://github.com/msukmanowsky/OmnitureDataFileInputFormat) well as the UDF.

    REGISTER path/to/jar/OmnitureDataInputFileFormat.jar
    REGISTER path/to/jar/OmnitureTextLoader.jar

From here, there are three ways to interface with the UDF when loading Omniture data.

### Read in all 226 fields per tuple
In this case you'll have to specify the field definitions for all 226 fields.  I'll update this readme soon to include this reference so it isn't so labourious (you could also e-mail me if you'd like :)).

    A = LOAD '/path/to/data/hit_data.tsv' USING com.tgam.hadoop.pig.OmnitureTextLoader() AS (hit_time_gmt:long, service:chararray, ...)
    
### Select fields by name
You can select fields by the field name specified in Omniture's documentation.

    A = LOAD '/path/to/data/hit_data.tsv' USING com.tgam.hadoop.pig.OmnitureTextLoader('hit_time_gmt', 'visid_high', 'visid_low', 'geo_city') AS (hit_time_gmt:long, visid_high:chararray, visid_low:chararray, city:chararray)
    
### Select fields by index
If you'd prefer to select fields by the index number you know is assigned to them, you can also do that.

    A = LOAD '/path/to/data/hit_data.tsv' USING com.tgam.hadoop.pig.OmnitureTextLoader(0, 12, 44, 22) AS (hit_time_gmt:long, ...)