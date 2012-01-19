package com.tgam.hadoop.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.tgam.hadoop.mapreduce.OmnitureDataFileInputFormat;
import com.tgam.hadoop.mapreduce.OmnitureDataFileRecordReader;

/**
 * A Pig UDF for reading and parsing raw Omniture data supplied via data feed (hit_data.tsv). 
 * @author Mike Sukmanowsky (<a href="mailto:mike.sukmanowsky@gmail.com">mike.sukmanowsky@gmail.com</a>)
 *
 */
public class OmnitureTextLoader extends LoadFunc {
	
	protected RecordReader reader = null;
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private ArrayList <Integer>selectedFields = new ArrayList <Integer>();

	public static final String DELIMITER = "\t";
	// TODO: eventually update for added props and click tag support?
	public static final String[] SFIELDS = {"hit_time_gmt", "service", "accept_language", "date_time", "visid_high", "visid_low", "event_list", "homepage", "ip", "page_event", "page_event_var_1", "page_event_var_2", "page_type", "page_url", "pagename", "product_list", "user_server", "channel", "prop1", "prop2", "prop3", "prop4", "prop5", "prop6", "prop7", "prop8", "prop9", "prop10", "prop11", "prop12", "prop13", "prop14", "prop15", "prop16", "prop17", "prop18", "prop19", "prop20", "prop21", "prop22", "prop23", "prop24", "prop25", "prop26", "prop27", "prop28", "prop29", "prop30", "prop31", "prop32", "prop33", "prop34", "prop35", "prop36", "prop37", "prop38", "prop39", "prop40", "prop41", "prop42", "prop43", "prop44", "prop45", "prop46", "prop47", "prop48", "prop49", "prop50", "purchaseid", "referrer", "state", "user_agent", "zip", "search_engine", "exclude_hit", "hier1", "hier2", "hier3", "hier4", "hier5", "browser", "post_browser_height", "post_browser_width", "post_cookies", "post_java_enabled", "post_persistent_cookie", "color", "connection_type", "country", "domain", "post_t_time_info", "javascript", "language", "os", "plugins", "resolution", "last_hit_time_gmt", "first_hit_time_gmt", "visit_start_time_gmt", "last_purchase_time_gmt", "last_purchase_num", "first_hit_page_url", "first_hit_pagename", "visit_start_page_url", "visit_start_pagename", "first_hit_referrer", "visit_referrer", "visit_search_engine", "visit_num", "visit_page_num", "prev_page", "geo_city", "geo_country", "geo_region", "duplicate_purchase", "new_visit", "daily_visitor", "hourly_visitor", "monthly_visitor", "yearly_visitor", "post_campaign", "evar1", "evar2", "evar3", "evar4", "evar5", "evar6", "evar7", "evar8", "evar9", "evar10", "evar11", "evar12", "evar13", "evar14", "evar15", "evar16", "evar17", "evar18", "evar19", "evar20", "evar21", "evar22", "evar23", "evar24", "evar25", "evar26", "evar27", "evar28", "evar29", "evar30", "evar31", "evar32", "evar33", "evar34", "evar35", "evar36", "evar37", "evar38", "evar39", "evar40", "evar41", "evar42", "evar43", "evar44", "evar45", "evar46", "evar47", "evar48", "evar49", "evar50", "post_evar1", "post_evar2", "post_evar3", "post_evar4", "post_evar5", "post_evar6", "post_evar7", "post_evar8", "post_evar9", "post_evar10", "post_evar11", "post_evar12", "post_evar13", "post_evar14", "post_evar15", "post_evar16", "post_evar17", "post_evar18", "post_evar19", "post_evar20", "post_evar21", "post_evar22", "post_evar23", "post_evar24", "post_evar25", "post_evar26", "post_evar27", "post_evar28", "post_evar29", "post_evar30", "post_evar31", "post_evar32", "post_evar33", "post_evar34", "post_evar35", "post_evar36", "post_evar37", "post_evar38", "post_evar39", "post_evar40", "post_evar41", "post_evar42", "post_evar43", "post_evar44", "post_evar45", "post_evar46", "post_evar47", "post_evar48", "post_evar49", "post_evar50", "click_action", "click_action_type", "click_context", "click_context_type", "click_sourceid"};
	public static final ArrayList <String>FIELDS = new ArrayList <String>(Arrays.asList(SFIELDS));
	
	/**
	 * Default constructor, will return all fields of every record per tuple.
	 * @return An OmnitureTextLoader which will return all fields per tuple.
	 */
	public OmnitureTextLoader() {
		for (int i = 0; i < FIELDS.size(); i++) {
			selectedFields.add(new Integer(i));
		}
	}
	
	/**
	 * Limit the tuples returned to contain only those fields specified.  Field names must reference a member of {@link OmnitureTextLoader.SFIELDS}.
	 * You can also specify a field by its numeric index by using {@link OmnitureTextLoader(int[])}
	 * @param fields A string array containing the specific fields to return in tuples. 
	 * @throws Exception 
	 */
	public OmnitureTextLoader(String...fields) throws Exception {
		for (String field : fields) {
			int index = FIELDS.indexOf(field);
			if (index == -1)
				throw new Exception("No field with name " + field + " exists.  Please check the OmnitureTextLoader.SFIELDS JavaDoc for a field reference.");
			
			selectedFields.add(new Integer(index));
		}
	}
	
	/**
	 * Limit the tuples returned to contain only those specified by numeric index.  For reference, consult {@link OmnitureTextLoader.SFIELDS}. 
	 * @param fieldIndexes
	 */
	public OmnitureTextLoader(int...fieldIndexes) throws IndexOutOfBoundsException {
		for(int index : fieldIndexes) {
			if (index < 0 || index > FIELDS.size()) 
				throw new IndexOutOfBoundsException("There are only " + FIELDS.size() + " fields available.  Please select within the range 0 - " + FIELDS.size());
			
			selectedFields.add(new Integer(index));
		}
	}
	

	@Override
	/**
	 * Provides an input format for the Loader to use.
	 */
	public OmnitureDataFileInputFormat getInputFormat() throws IOException {
		return new OmnitureDataFileInputFormat();
	}

	@Override
	public Tuple getNext() throws IOException {
		Tuple tuple = null;
		List<String> values = new ArrayList<String>();
		
		try {
			boolean notDone = reader.nextKeyValue();
			if (!notDone)
				return null;
			
			Text value = (Text)reader.getCurrentValue();
			
			if (value != null) {
				String[] parts = value.toString().split(OmnitureTextLoader.DELIMITER);
				
				if (parts.length != FIELDS.size())
					throw new IOException("Error when parsing line (" + value.toString() + ").  Found " + parts.length + " fields but expected " + FIELDS.size() + ".");
			
				for (int i = 0; i < selectedFields.size(); i++) {
					values.add(parts[(int)selectedFields.get(i)]);
				}
			
				tuple = tupleFactory.newTuple(values);
			}
		} catch (InterruptedException e) {
			int errorCode = 6018;
			String errorMsg = "Error while reading input...";
			throw new ExecException(errorMsg, errorCode, PigException.REMOTE_ENVIRONMENT, e);
		}
		
		return tuple;
	}

	@Override
	public void prepareToRead(RecordReader recordReader, PigSplit pigSplit)
			throws IOException {
		this.reader = recordReader;
		// Don't need to store the pigSplit for this reader
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);
	}
}
