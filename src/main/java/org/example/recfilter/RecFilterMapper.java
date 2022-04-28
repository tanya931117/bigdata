package org.example.recfilter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RecFilterMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 将这一行文本转为字符数组accountid+"\u0001"+recJobs
        String[] strs = value.toString().split("\u0001");
        String accountid=strs[0];
        String recJobs=strs[1];
        context.write(new Text(accountid), new Text(recJobs));
    }
}


