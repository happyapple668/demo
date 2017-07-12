package com.gavin.mapreduce.table2join;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Table2JoinMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		if(value == null) {
			return;
		}
		//获取当前正在处理的文件名
		String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
		//分隔值
		String vals[] = value.toString().split(",");

		if(fileName.startsWith("file01")) { //table1的数据
			context.write(new Text(vals[1]), new Text("1"+vals[0])); //写入b作为key的值,前面的1表示是左表数据
		} else { //table2的数据
			context.write(new Text(vals[0]), new Text("2"+vals[1])); //写入b作为key的值,前面的2表示是右表数据
		}
	}


}
