package hadoop.cs499a3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, IntWritable>{

	/**
	 * Method which performs the reduce operation and sums 
	 * all the occurrences of the word before passing it to be stored in output
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
	
		int sum = 0;
		Iterator<Text> valuesIt = values.iterator();
		// Checking the chain of a hash.
		// IMPORTANT: valuesIt needs to set next
		while(valuesIt.hasNext()){
			valuesIt.next();
			sum = sum + 1;
		}
		
		// Write the Key followed by the count
		context.write(key, new IntWritable(sum));
	}	
}