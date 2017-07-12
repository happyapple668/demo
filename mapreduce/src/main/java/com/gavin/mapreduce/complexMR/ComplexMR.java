package com.gavin.mapreduce.complexMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 SELECT IP, CMD, COUNT(*) CMDTOTAL
 , SUM(COUNT(*)) OVER(PARTITION BY IP) IPTOTAL
 FROM
 GROUP BY IP, CMD
 */
public class ComplexMR {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "ComplexMR job");
        job.setJarByClass(ComplexMR.class);

        job.setInputFormatClass(TextInputFormat.class); //设置输入格式
        job.setOutputFormatClass(TextOutputFormat.class);//设置输出格式

        job.setMapperClass(ComplexMRMapper.class); //设置Mapper类
        job.setReducerClass(ComplexMRReducer.class);//设置Reducer类

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class); //设置输出的KEY类型
        job.setOutputValueClass(Text.class);//设置输出的VALUE类型

        FileInputFormat.setInputPaths(job, new Path("D:\\workspace\\demo\\mapreduce\\src\\main\\resources"));//设置输入文件路径
        FileOutputFormat.setOutputPath(job, new Path("D:\\workspace\\demo\\out\\hdfsoutput"));//设置输出文件路径

        job.waitForCompletion(true);

    }

}
