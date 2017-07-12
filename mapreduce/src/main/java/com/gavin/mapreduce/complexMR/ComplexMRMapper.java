package com.gavin.mapreduce.complexMR;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Gavin on 2017/6/4.
 */
public class ComplexMRMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) {
            return;
        }
        String[] line = value.toString().trim().split("\t");
        String ip = line[2].substring(4).trim();
        String cmd = line[3].substring(4).trim();
        try {
            context.write(new Text(ip), new Text(cmd));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
