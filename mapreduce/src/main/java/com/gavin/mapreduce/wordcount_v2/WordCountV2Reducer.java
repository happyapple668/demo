package com.gavin.mapreduce.wordcount_v2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountV2Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/*
	 * Reduce的输入KEY,VALUE与MAP的输出KEY,VALUE就该是一致的。
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
						  Context context) throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable iv : values) {
			count += iv.get();
		}

		context.write(key, new IntWritable(count));
	}



}
