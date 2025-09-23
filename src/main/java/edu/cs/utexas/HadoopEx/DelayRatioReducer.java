package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelayRatioReducer extends  Reducer<Text, Text, Text, Text> {

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
       int countSum = 0;
       int delaySum = 0;
       
       for (Text value : values) {
           String[] parts = value.toString().split(",");
           if (parts.length == 2) {
               delaySum += Integer.parseInt(parts[0]);  
               countSum += Integer.parseInt(parts[1]);  
           }
       }
       
       context.write(text, new Text(delaySum + "," + countSum));
   }
}