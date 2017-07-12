package com.gavin.mapreduce.wordcount_v2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountV2Mapper 
	extends Mapper<Object/*input key*/, Text/*input value*/, Text/*output key*/, IntWritable/*output value*/>{

	private final IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(Object key/*input key*/, Text value/*input value*/, Context context)
			throws IOException, InterruptedException {
		if(value == null) {
			return;
		}
		String[] vals = value.toString().split(" ");
		for(String val : vals) {
			context.write(new Text(val), one);
		}
	}

	
}
