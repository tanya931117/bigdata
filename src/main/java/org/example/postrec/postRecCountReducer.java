package org.example.postrec;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class postRecCountReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // accountid+"\t"+post/rec:jobs(jobid1+"\u0002"+jobid2...)
        String postJobs ="";
        String recJobs ="";
        for(Text value: values){
            String jobs = value.toString();
            if(jobs.contains("post")){//投递
                postJobs = jobs.split(":")[1];
            }
            if(jobs.contains("rec")){//推荐
                recJobs = jobs.split(":")[1];
            }
        }
        if (postJobs.length()>0 && recJobs.length()>0){
            String userJobs = postJobs+"\t"+recJobs;
            context.write(key, new Text(userJobs));
        }
    }
}
