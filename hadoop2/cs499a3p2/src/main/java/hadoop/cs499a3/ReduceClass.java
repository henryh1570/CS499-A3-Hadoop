package hadoop.cs499a3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceClass extends Reducer<Text, Text, Text, DoubleWritable>{

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
	
		int size = 0;
		double score = 0.0;
		Iterator<Text> valuesIt = values.iterator();
		// Checking the chain of a hash.
		// IMPORTANT: valuesIt needs to set next
		while(valuesIt.hasNext()){
			size++;
			// Sum the score extracted from the next Key,Value pair
			score += Double.parseDouble(valuesIt.next().toString().split(",")[2]);
		}
		
		// Write the Key followed by the average score
		context.write(key, new DoubleWritable(score / size));
	}	
}