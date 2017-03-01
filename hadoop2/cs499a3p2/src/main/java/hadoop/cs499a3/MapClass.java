package hadoop.cs499a3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Input 1 line at a time.
// Mapper takes in LogWritable Key and Text as Input
// Mapper outputs a Text Key and Text Output
public class MapClass extends Mapper<LongWritable, Text, Text, Text> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	/**
	 * map function of Mapper parent class takes a line of text at a time splits
	 * to tokens and passes to the context as word along with value as one
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Get the next single line from file.
		String line = value.toString();
		String index = getIndex(line);
		if (index != "-1") {
			word.set(index);
			// Write the entire line as value
			Text text = new Text(line);
			context.write(word, text);
		}
	}

	// Parse the index of string, return as key.
	// Specifically, using MOVIE ID as key.
	private String getIndex(String str) {
		try {
			String[] arr = str.split(",");
			return arr[0];
		} catch (Exception e) {
			return "-1";
		}
	}
}
