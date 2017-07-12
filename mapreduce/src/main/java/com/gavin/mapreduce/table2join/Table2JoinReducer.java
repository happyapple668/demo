package com.gavin.mapreduce.table2join;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Table2JoinReducer extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> values,
						  Context context) throws IOException, InterruptedException {
		//存储table 01表的列值
		List<String> file01Values = new LinkedList<String>();
		//存储table 02的列值
		List<String> file02Values = new LinkedList<String>();
		for(Text v : values) {
			String val = v.toString();
			if(val.charAt(0) == '1') {
				file01Values.add(val.substring(1));  //截取后面的值
			} else {
				file02Values.add(val.substring(1));
			}
		}

		//写入关联后的列值
		for(String f1 : file01Values) {
			for(String f2 : file02Values) {
				context.write(key, new Text(f1 + '\t' + f2));
			}
		}
	}



}
