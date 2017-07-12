package com.gavin.mapreduce.wordcount_v1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


/**
 * MR V1的实现
 * MR V1的类主要在org.apache.hadoop.mapred包下
 */
public class WordCount {

	public static void main(String[] args) throws Exception  {
		JobConf jobConf = new JobConf(WordCount.class);
		jobConf.setJobName("World count v1");

		jobConf.setOutputKeyClass(Text.class); //设置输出的KEY类型
		jobConf.setOutputValueClass(IntWritable.class);//设置输出的VALUE类型

		jobConf.setMapperClass(WordCountMapper.class); //设置MAPPER类
		jobConf.setCombinerClass(WordCountReducer.class); //设置Combiner 类
		jobConf.setReducerClass(WordCountReducer.class); //设置Reducer 类

		jobConf.setInputFormat(TextInputFormat.class); //设置输入数据格式
		jobConf.setOutputFormat(TextOutputFormat.class);//设置输出数据格式

		FileInputFormat.setInputPaths(jobConf, new Path("C:\\sftp\\sftp\\")); //设置输入文件路径
		FileOutputFormat.setOutputPath(jobConf, new Path("C:\\sftp\\logs\\a"));//设置输出文件路径
		//运行任务
		JobClient.runJob(jobConf);

	}

}
