package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Iterator;



public class Top3Reducer extends  Reducer<Text, IntWritable, Text, IntWritable> {

    private PriorityQueue<FlightAndCount> pq = new PriorityQueue<FlightAndCount>(3);;


    private Logger logger = Logger.getLogger(Top3Reducer.class);


//    public void setup(Context context) {
//
//        pq = new PriorityQueue<FlightAndCount>(10);
//    }


    /**
     * Takes in the top3 from each mapper and calculates the overall top3
     * @param text
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
   public void reduce(Text key, Iterable<IntWritable> values, Context context)
           throws IOException, InterruptedException {


       // A local counter just to illustrate the number of values here!
        int counter = 0 ;


       // size of values is 1 because key only has one distinct value
       for (IntWritable value : values) {
           counter = counter + 1;
           logger.info("Reducer Text: counter is " + counter);
           logger.info("Reducer Text: Add this item  " + new FlightAndCount(key, value).toString());

           pq.add(new FlightAndCount(new Text(key), new IntWritable(value.get()) ) );

           logger.info("Reducer Text: " + key.toString() + " , Count: " + value.toString());
           logger.info("PQ Status: " + pq.toString());
       }

       // keep the priorityQueue size <= heapSize
       while (pq.size() > 3) {
           pq.poll();
       }


   }


    public void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("Top3Reducer cleanup cleanup.");
        logger.info("pq.size() is " + pq.size());

        List<FlightAndCount> values = new ArrayList<FlightAndCount>(3);

        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        logger.info("values.size() is " + values.size());
        logger.info(values.toString());


        // reverse so they are ordered in descending order
        Collections.reverse(values);


        for (FlightAndCount value : values) {
            context.write(value.getWord(), value.getCount());
            logger.info("Top3Reducer - Top-3 Airports are:  " + value.getWord() + "  Count:"+ value.getCount());
        }


    }

}