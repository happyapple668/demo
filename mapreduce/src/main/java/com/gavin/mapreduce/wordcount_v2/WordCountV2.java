package com.gavin.mapreduce.wordcount_v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * MR V2的实现
 * MR V2的类主要在org.apache.hadoop.mapreduce包下
 */
public class WordCountV2 {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set(name, value);

		Job job = Job.getInstance(conf, "word count v2");
		job.setJarByClass(WordCountV2.class);


		FileInputFormat.setInputPaths(job, new Path("C:\\sftp\\sftp\\"));//设置输入文件路径
		FileOutputFormat.setOutputPath(job, new Path("C:\\sftp\\logs\\a"));//设置输出文件路径

		job.setMapperClass(WordCountV2Mapper.class); //设置Mapper类
		job.setReducerClass(WordCountV2Reducer.class);//设置Reducer类
//		job.setCombinerClass(WordCountV2Reducer.class);//设置Combiner类

		job.setInputFormatClass(TextInputFormat.class); //设置输入格式
		job.setOutputFormatClass(TextOutputFormat.class);//设置输出格式


		//设置最终REDUCE输出的值类型
		job.setOutputKeyClass(Text.class); //设置输出的KEY类型
		job.setOutputValueClass(IntWritable.class);//设置输出的VALUE类型

		if(args.length >= 3) {
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}

		job.waitForCompletion(true);
//		job.submit();
	}

}
