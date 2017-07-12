package com.gavin.mapreduce.distinct;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DistinctReducer extends Reducer<Text, Text, Text, Text>{

	Text empty = new Text("");
	
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		context.write(key, empty);
		
	}

	
	
}
