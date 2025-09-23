package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class Top3Mapper extends Mapper<Text, Text, Text, IntWritable> {

	private Logger logger = Logger.getLogger(Top3Mapper.class);


	private PriorityQueue<FlightAndCount> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();

	}

	/**
	 * Reads in results from the first job and filters the top3 results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {


		int count = Integer.parseInt(value.toString());

		pq.add(new FlightAndCount(new Text(key), new IntWritable(count)) );

		if (pq.size() > 3) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			FlightAndCount flightAndCount = pq.poll();
			context.write(flightAndCount.getWord(), flightAndCount.getCount());
			logger.info("Top3Mapper PQ Status: " + pq.toString());
		}
	}

}