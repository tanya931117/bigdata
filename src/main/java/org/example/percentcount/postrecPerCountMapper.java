package org.example.percentcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class postrecPerCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // accountid+"\t"+postJobs(jobid1+"\u0002"+jobid2...)+"\t"+recJobs(jobid1+"\u0002"+jobid2...)
        String str = value.toString();
        String[] strs = str.split("\t");
        String accountid = strs[0];
        String postJobs = strs[1];
        String recJobs = strs[2];
        context.write(new Text(accountid), new Text(postJobs + "\u0001" + recJobs));
    }
}
