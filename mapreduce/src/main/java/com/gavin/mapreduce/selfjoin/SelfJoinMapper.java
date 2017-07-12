package com.gavin.mapreduce.selfjoin;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SelfJoinMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		if(value == null) {
			return;
		}
		String vals[] = value.toString().split(",");
		context.write(new Text(vals[1]), new Text("1"+vals[0])); //写入b作为key的值,前面的1表示是左表数据
		context.write(new Text(vals[0]), new Text("2"+vals[1])); //写入a作为key的值,前面的2表示是右表数据
	}


}
