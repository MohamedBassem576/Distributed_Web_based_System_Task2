package DW_Assign1_Task2;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import java.io.IOException;
public class Assign1_Task2_Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	
	  private Text c = new Text();//country name datatype
	   private FloatWritable s = new FloatWritable(); //salary datatype
	// override the reduce method 
		@Override


	  
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
   String line = value.toString();
   String [] x = line.split(",");         
   //splitting the text with a comma so that we can access the value 
   if (x.length >= 4) {
       String Name_of_Country = x[3].trim(); // we remove the whitespace from both ends of a string and returns a new string, without changing the original string.
       try {
           float salary = Float.parseFloat(x[2].trim());
           c.set(Name_of_Country);//set the country name in the mapper
           s.set(salary);//set the salary in the mapper
           context.write(c, s);
       } catch (NumberFormatException e) {
           // Ignore invalid salary values

	  }

}
}
 
}