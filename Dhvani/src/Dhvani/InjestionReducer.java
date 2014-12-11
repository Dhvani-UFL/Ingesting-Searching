package Dhvani;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class InjestionReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
          
        String fileLocation = " ";
        
        String errorMessage = "File Location Not Found";
        
        //To get file location value 
        
        if(values.hasNext()) {
           fileLocation = values.next.get();
          }
        
        //Store file in String array
        
        String[] file;
        file[0] = fileLocation;
        
        
        //Trigger ingestion on each song thus obtained
        
        try{
        	Ingest.ingestFP(file);
        }
        catch(NullPointerException e){
        	e.printStackTrace();
        	System.out.println("Error Message:    " + errorMessage);
        }
        
        output.collect(key, new Text(fileLocation));
        
        }
      }


