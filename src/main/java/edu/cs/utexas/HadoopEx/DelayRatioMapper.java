package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayRatioMapper extends Mapper<Object, Text, Text, Text> {

	// Create a counter and initialize with 1
	private final IntWritable counter = new IntWritable(1);
	// Create a hadoop text object to store words
	private Text word = new Text();

	private Text compositeValue = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		// Convert the line to string
		String line = value.toString().trim();
		
		if (line.isEmpty() || line.startsWith("YEAR")) {
			return;
		}
		
		String[] fields = line.split(",");
		
=		if (fields.length >= 12) {
			String airline = fields[4].trim();
			
			// Parse delay field (column index 11) as integer
			try {
				int delay = Integer.parseInt(fields[11].trim());
				
				// Only process non-empty airline codes
				if (!airline.isEmpty()) {
					word.set(airline);
					// Create composite value: delay + "," + counter
					compositeValue.set(delay + "," + counter.get());
					context.write(word, compositeValue);
				}
			} catch (NumberFormatException e) {
				return;
			}
		}
	}
}