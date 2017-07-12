package com.gavin.mapreduce.complexMR;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Gavin on 2017/6/4.
 */
public class ComplexMRReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) {

        int i = 0; // 记录sum(count(*)) over(partition by ip)
        Map<String, Integer> map = new HashMap<String,Integer>(); // 记录操作次数 记录 count(*)
        for (Text v : values) {
            i++;
            if(map.containsKey(v.toString())){
                map.put(v.toString(),map.get(v.toString()) + 1);
            } else{
                map.put(v.toString(),1);
            }
        }
        String str = "";//输出的value
        for(String s : map.keySet()) {
            str = s + ",count(*)=" + map.get(s) + ",sum() over(by ip)=" + i;
            try {
                context.write(key, new Text(str));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
