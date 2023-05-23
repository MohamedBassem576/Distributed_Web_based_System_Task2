package DW_Assign1_Task2;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Assign1_Task2_Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	  
	// declare a FloatWritable to store the result
		private FloatWritable result = new FloatWritable();

		// override the reduce method 
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			
			// initialize variables to keep track of sum and count
			float sum = 0.0f;
			int count = 0;

			// iterate over the values for the current key
			for (FloatWritable value : values) {
				// add the value to the sum
				sum += value.get();
				
				count++;
			}

			// calculate the average salary
			float average = sum / count;

			// set the result to the calculated average
			result.set(average);

			// write the key-value pair to the output context
			context.write(key,result);
		}

	


	
}



