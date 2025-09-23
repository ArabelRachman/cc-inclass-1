package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.PriorityQueue;

public class DelayRatioTop3AirlineReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private PriorityQueue<DelayRatioAndCount> pq = new PriorityQueue<DelayRatioAndCount>(3);

    private Logger logger = Logger.getLogger(DelayRatioTop3AirlineReducer.class);

    /**
     * Takes in the top3 from each mapper and calculates the overall top3 airlines by delay ratio
     * @param key     airline code
     * @param values  delay ratio values
     * @param context hadoop context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        int counter = 0;

        for (DoubleWritable value : values) {
            counter = counter + 1;

            pq.add(new DelayRatioAndCount(new Text(key), new DoubleWritable(value.get())));

            if (pq.size() > 3) {
                pq.poll();
            }
        }

        logger.info("Key " + key.toString() + " has " + counter + " values");
    }

    public void cleanup(Context context) throws IOException, InterruptedException {

        logger.info("cleanup method has been called " + pq.size());

        while (pq.size() > 0) {
            DelayRatioAndCount delayRatioAndCount = pq.poll();
            logger.info("Top Airline (by delay ratio): " + delayRatioAndCount.getAirline() + 
                       " with delay ratio: " + delayRatioAndCount.getDelayRatio());

            context.write(delayRatioAndCount.getAirline(), delayRatioAndCount.getDelayRatio());
        }
    }
}