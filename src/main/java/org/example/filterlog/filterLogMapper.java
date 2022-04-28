package org.example.filterlog;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class filterLogMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        // accountid+"\t"+postJobs(jobid1+"\u0002"+jobid2...)+"\t"+recJobs(jobid1+"\u0002"+jobid2...)
        String str = value.toString();
        try{
            JSONObject obj = JSON.parseObject(str);
            String project = obj.getString("project");
            String type = obj.getString("type");
            String event = obj.getString("event");
            context.write(new Text(type+":"+project+":"+event), new Text(str));
        }catch (JSONException e){
            System.out.println(str);
            e.printStackTrace();
        }
    }
}
