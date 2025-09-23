package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.PriorityQueue;

public class DelayRatioTop3AirlineMapper extends Mapper<Text, Text, Text, DoubleWritable> {

    private Logger logger = Logger.getLogger(DelayRatioTop3AirlineMapper.class);

    private PriorityQueue<DelayRatioAndCount> pq;

    public void setup(Context context) {
        pq = new PriorityQueue<>();
    }

    /**
     * Reads in results from the first job and filters the top3 results based on delay ratio
     *
     * @param key   airline code
     * @param value composite value "totalDelay,totalCount"
     */
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split(",");
        if (parts.length == 2) {
            try {
                double totalDelay = Double.parseDouble(parts[0]);
                double totalCount = Double.parseDouble(parts[1]);
                
                double delayRatio = totalCount > 0 ? totalDelay / totalCount : 0.0;

                pq.add(new DelayRatioAndCount(new Text(key), new DoubleWritable(delayRatio)));

                if (pq.size() > 3) {
                    pq.poll();
                }
            } catch (NumberFormatException e) {
                logger.warn("Malformed record: " + key + " -> " + value);
            }
        }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
        while (pq.size() > 0) {
            DelayRatioAndCount delayRatioAndCount = pq.poll();
            context.write(delayRatioAndCount.getAirline(), delayRatioAndCount.getDelayRatio());
        }
    }
}