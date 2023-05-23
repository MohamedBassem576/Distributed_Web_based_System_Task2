package DW_Assign1_Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class Assign1_Task2 {

 public static void main(String[] args) throws Exception{
	 if (args.length != 2) {
	      System.err.println("Usage: MaxTemperature <input path> <output path>");
	      System.exit(-1);
	    }
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Average salary");

	   
	    job.setJarByClass(Assign1_Task2.class);
	    job.setMapperClass(Assign1_Task2_Mapper.class);
	    job.setReducerClass(Assign1_Task2_Reducer .class);
	    

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);

	    System.exit(job.waitForCompletion(true) ? 0 : 1);

 }
 
}









