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
        
        //To get file location value 
        if(values.hasNext()) {
           fileLocation = values.next.get();
          }
        
        //Trigger ingestion on each song thus obtained
        ingestFP(fileLocation);
        
        output.collect(key, new Text(fileLocation));
        
        }
      }


