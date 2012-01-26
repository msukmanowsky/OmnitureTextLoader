import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;

import com.tgam.hadoop.pig.OmnitureTextLoader;
import com.tgam.hadoop.util.EscapedLineReader;

public class OmnitureTextLoaderTester {
	
	private MockRecordReader reader;
	private OmnitureTextLoader loader;
	
	@Before
	public void setUp() throws IOException {
		// TODO: Should test with an escaped new line/tab file as well as a non-escaped file - or just have the first and second rows with one escaped and one non-escaped
		reader = new MockRecordReader("test/hit_data.tsv");
		loader = new OmnitureTextLoader();
		loader.prepareToRead(reader, null);
	}
	
	@Test
	// TODO: Fix this - this unit test doesn't really work.  Keep getting OutOfMemoryErrors which make no sense.
	public void topVisitorsByPageViews() throws IOException, ParseException {		
		PigTest test = new PigTest("test/page_views_by_visitor.pig");
		
		String[] input = {
			"1130770920	ss	en-ca	2005-11-01 00:02:00	675	463		U	161.114.64.75	0				http://netscape.www.electronics.com/Accessories	Category - Accessories		netscape.www	Accessories																																																				http://www.electronics.com/		Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0)		0	0						207	560	99	Y	Y	Y	3	1	179	pr5-ts5.telepac.pt	31/09/2005 12:-1:00 1 300	4	39	23		6	1130770800	1097017200	1130770800	1096801200	1	http://www.electronics.com/Software/NortonSystemWorks2002PersonalFirewallBundle	Norton SystemWorks 2002 / Personal Firewall Bundle	http://www.electronics.com/	Home Page	http://search.msn.com/?t=electronics.com	http://www.google.com/search/?term=www.electronics.com	57	4	2	2	cambridge	usa	ma	0	0	0	0	0	0	516:2																																																			Registered	Home Page: Header Product Section	Liquidation Center	Viewsonic Computer Monitors	Successful Basic Search terms	Normal	Commission Junction	Magazine Ad	1.00	40-44				N		0128100000000213																																				0		0	0	",
			"1130775150	ss	en-us	2005-11-01 01:12:30	616	595	12,10	U	213.219.8.227	0				http://www.electronics.com/CartAdd	Add Product To Cart	Computers;HP 511N CEL 1.3 128/40;1;649.91;211=42.24801|212=16.24921|213=434.78	www	Initial Buy Process																																																				http://www.electronics.com/Computers/HP511NCEL1.3		Mozilla/4.0 (compatible; MSIE 6.0; Windows 98)		0	0						261	710	1010	Y	Y	Y	3	2	300	spider-mtc.th037.proxy.aol.com	31/09/2005 00:-1:00 1 -480	4	45	8		6	1130774400	1087207200	1130774400	1087077600	0	http://www.electronics.com/	Home Page	http://www.electronics.com/Computers/HP511NCEL1.3	Product - HP 511N CEL 1.3 128/40	http://search.msn.com/?t=electronics.com	http://www.google.com/search/?term=www.electronics.com&p=Laptop Computers&q=Laptop Computers&query=Laptop Computers&ask=Laptop Computers	57	6	2	73	aol	gbr	aol	0	0	0	0	0	0	389:4																																																			Registered	Enter Site Pop-up	Free Shipping on orders over $99	Wireless Network adapters	Successful Basic Search terms	1-click	Commission Junction	Friend	1.00	18-24				N		0128100000000149																																				0		0	0	",
			"1130775170	ss	en-us	2005-11-01 01:12:50	616	595	11	U	213.219.8.227	0				http://www.electronics.com/CustomerInformation	Buy Process - Customer Information	Computers;HP 511N CEL 1.3 128/40;1;649.91;211=42.24801|212=16.24921|213=434.78	www	Initial Buy Process																																																				http://www.electronics.com/CartAdd		Mozilla/4.0 (compatible; MSIE 6.0; Windows 98)		0	0						261	710	1010	Y	Y	Y	3	2	300	spider-mtc.th037.proxy.aol.com	31/09/2005 00:-1:00 1 -480	4	45	8		6	1130775150	1087207200	1130774400	1087077600	0	http://www.electronics.com/	Home Page	http://www.electronics.com/Computers/HP511NCEL1.3	Product - HP 511N CEL 1.3 128/40	http://search.msn.com/?t=electronics.com	http://www.google.com/search/?term=www.electronics.com&p=Laptop Computers&q=Laptop Computers&query=Laptop Computers&ask=Laptop Computers	57	6	3	6	aol	gbr	aol	0	0	0	0	0	0	389:4																																																			Registered	Enter Site Pop-up	Free Shipping on orders over $99	Wireless Network adapters	Successful Basic Search terms	1-click	Commission Junction	Friend	1.00	18-24				N		0128100000000149																																				0		0	0	"
		};
		
		// TODO: This is not the correct output for hit_data.tsv, it is for input above
		String[] output = {
			"72239,24",
			"482184,11",
			"713877,10",
			"544423,7",
			"240249,6"
		};
		
		test.assertOutput("data", input, "output", output);
	}
	
	@Test
	public void testAllFields() throws Exception {
		Tuple tuple = loader.getNext();
		
		// Check that the tuple is structurally correct first
		assertNotNull(tuple);
		assertEquals(tuple.size(), 226);
		
		// Now we'll check that the tuple contains some of the right values
		String language = (String)tuple.get(2);
		String ipAddress = (String)tuple.get(8);
				
		assertEquals("en-ca", language);
		assertEquals("161.114.64.75", ipAddress);
	}
	
	@Test
	public void testEventListBag() throws Exception {
		// Skip the first row
		loader.getNext();
		Tuple t = loader.getNext();
		
		assertNotNull(t);
		assertEquals(t.size(), 226);
		
		DataBag event_list = (DataBag)t.get(6);
		Iterator<Tuple> it = event_list.iterator();
		Tuple temp = it.next();
		assertEquals(temp.get(0).toString(), "12");
		temp = it.next();
		assertEquals(temp.get(0).toString(), "10");
	}
		
	public class MockRecordReader extends RecordReader {
		private EscapedLineReader lineReader;
		private long key;
		private boolean hasLinesLeft;
		
		/**
		 * Call this to load the file
		 * @param fileLocation
		 * @throws FileNotFoundException
		 */
		public MockRecordReader(String fileLocation) throws FileNotFoundException {
			lineReader = new EscapedLineReader(new FileInputStream(fileLocation));
			key = 0;
			hasLinesLeft = true;
		}
		
		@Override
		public void close() throws IOException { }
		
		@Override
		public Object getCurrentKey() throws IOException, InterruptedException {
			return key;
		}
		
		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			int bytesRead;
			
			Text value = new Text();
			bytesRead = lineReader.readLine(value);
			key += (long)bytesRead;
			
			if (bytesRead == 0) {
				hasLinesLeft = false;
			}
			
			return value;
		}
		
		@Override
		public float getProgress() throws IOException, InterruptedException {
			return 0;
		}
		
		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
			// No need to initialize
		}
		
		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return hasLinesLeft;
		}
		
	}
}
