package com.gavin.mapreduce.distinct;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 对文件里的行数据进行去重处理
 * SQL:
 * select distinct line
 * from file
 */
public class Distinct {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "distinct job");
		job.setJarByClass(Distinct.class);

		job.setInputFormatClass(TextInputFormat.class); //设置输入格式
		job.setOutputFormatClass(TextOutputFormat.class);//设置输出格式

		job.setMapperClass(DistinctMapper.class); //设置Mapper类
		job.setReducerClass(DistinctReducer.class);//设置Reducer类
		job.setCombinerClass(DistinctReducer.class);//设置Combiner类

		job.setOutputKeyClass(Text.class); //设置输出的KEY类型
		job.setOutputValueClass(Text.class);//设置输出的VALUE类型

		FileInputFormat.setInputPaths(job, new Path("C:/sftp/sftp/"));//设置输入文件路径
		FileOutputFormat.setOutputPath(job, new Path("C:/sftp/logs/a"));//设置输出文件路径

		if(args.length >= 3) {
			job.setNumReduceTasks(Integer.parseInt(args[2]));
		}

		job.waitForCompletion(true);
	}

}
