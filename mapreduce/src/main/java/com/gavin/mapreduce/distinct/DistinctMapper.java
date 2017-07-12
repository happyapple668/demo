package com.gavin.mapreduce.distinct;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistinctMapper extends Mapper<Object, Text, Text, Text>{

	Text empty = new Text("");
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		if(value == null) {
			return;
		}
		String[] strArr = value.toString().split(" "); // distinct 单词
		for(String str : strArr){
			context.write(new Text(str), empty);

		}
//		context.write(value, empty);  distinct 一行

	}

}
