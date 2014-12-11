package Dhvani;

import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reporter; 
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.mp3.Mp3Parser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


 
public class InjestionMapper extends MapReduceBase implements Mapper<FileInputFormat , Text, Text, Text> {
	/** 
	 * overriding mapper function 
	 */ 
	//The mapper will read each line from the input file which consists of 
	//s3 file location of each song in the given song list
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
	{ 
		    
		    	String fileLocation = "";
		    	
		    	String eachLine = value.toString();
		    	
		    	//retrieve the file location as first word of each line 
	            StringTokenizer tokenizer = new StringTokenizer(eachLine);
	            if(tokenizer.hasMoreTokens()) {
	            	fileLocation = tokenizer.nextToken();
	            }
		        
		        try {

		        InputStream inputStream = new FileInputStream(new File(fileLocation));
		        Parser Mp3 = new Mp3Parser();
		        
		        ContentHandler handler = new DefaultHandler();
		        Metadata data = new Metadata();
		        
		        ParseContext parsing = new ParseContext();
		        parser.parse(inputStream, handler, data, parsing);
		        
		        //input stream needs to be closed
		        inputStream.close();

		        //get meta data and retrieve song name for the same
		        String[] metadata = data.names();
		        
		        String title = metadata.get("title");
		        //thus each song will have id as its file location along with its name
		        fileLocation = fileLocation + title;
		        } 
		        catch (FileNotFoundException e) {
		        	e.printStackTrace();
		        }
		        catch (TikaException e) {
			        e.printStackTrace();
			    }
		        catch (SAXException e) {
		        	e.printStackTrace();
		        } 
		        catch (IOException e) {
		        	e.printStackTrace();
		        }
		        
	    output.collect(fileLocation, fileLocation);
	    	  
	} 

 


