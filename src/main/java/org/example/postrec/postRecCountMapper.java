package org.example.postrec;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class postRecCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        // accountid+"\u0001"+postJobs(jobid1+"\u0002"+jobid2...)
        // accountid+"\t"+recJobs(jobid1+"\u0002"+jobid2...)
        String str = value.toString();
        String accountid= "";
        String jobs= "";
        if (str.contains("\u0001")){
            //投递数据
            String[] strs = str.split("\u0001");
            accountid= strs[0];
            jobs= "post:"+strs[1];
        }else if (str.contains("\t")){
            //推荐数据
            String[] strs = str.split("\t");
            accountid= strs[0];
            jobs= "rec:"+strs[1];
        }
        context.write(new Text(accountid), new Text(jobs));
    }
}
