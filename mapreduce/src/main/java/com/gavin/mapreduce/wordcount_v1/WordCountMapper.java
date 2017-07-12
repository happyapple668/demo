package com.gavin.mapreduce.wordcount_v1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * Word count Map class
 */
public class WordCountMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
		if(value == null) {
			return;
		}
		String[] words = value.toString().split(" ");
		for(String word : words) {
			collector.collect(new Text(word), one);
		}
	}

}
