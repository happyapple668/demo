package com.gavin.mapreduce.wordcount_v1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * Word count  class
 */
public class WordCountReducer  extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

	public void reduce(Text key, Iterator<IntWritable> value, OutputCollector<Text, IntWritable> collector, Reporter reporter)
			throws IOException {
		int count = 0;
		for(; value.hasNext(); ) {
			count += value.next().get();
		}
		collector.collect(key, new IntWritable(count));
	}

}
