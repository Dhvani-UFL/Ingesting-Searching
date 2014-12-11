package Dhvani;
/** 
 *  
 * @author divchag
 * 
 */
public class InjestionMain {
	//This is the map reduce main program that will be triggered when we initially
	//dump hundreds of songs into database.
	//

	public static void main(String[] args) throws Exception {
          //job is to be configured with input location and output location
		  //as arguments to main function where the parameters will be set correspondingly.
		  //So the location of input directory in s3 bucket where songs are present 
		 
          JobConf job = new JobConf(InjestionMain.class);
          job.setJobName("INJESTIONMAIN");
    
          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);
    
          //Mapping mapper and reducer classes
          job.setMapperClass(InjestionMapper.class);
          job.setReducerClass(InjestionReducer.class);
    
          job.setInputFormat(TextInputFormat.class);
          job.setOutputFormat(TextOutputFormat.class);
    
          FileInputFormat.setInputPaths(job, new Path(args[0]));
          FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
          //triggering the configured job.
          JobClient.runJob(job);
        }

	}


