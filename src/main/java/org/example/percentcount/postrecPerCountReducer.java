package org.example.percentcount;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class postrecPerCountReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // postJobs(jobid1+"\u0002"+jobid2...)+"\u0001"+recJobs(jobid1+"\u0002"+jobid2...)
        for(Text value: values){
            String accountid = key.toString();
            String jobs = value.toString();
            String[] jobIds = jobs.split("\u0001");
            String[] postJobs = jobIds[0].split("\u0002");
            String[] recJobs = jobIds[1].split("\u0002");
            List<String> postJobIDList = Arrays.asList(postJobs);
            List<String> recJobIDList = Arrays.asList(recJobs);
            // 交集
            List<String> intersection = (List<String>) CollectionUtils.intersection(postJobIDList, recJobIDList);
            //投递职位数
            int post_size = postJobIDList.size();
            //推荐职位数
            int rec_size = recJobIDList.size();
            //投递推荐交集职位数
            int inter_size = intersection.size();
            //计算百分比
            java.text.DecimalFormat df=new java.text.DecimalFormat("##.##%");//传入格式模板
            String per=df.format((float)inter_size/(float)post_size);
            //拼接投递推荐交集职位ID
            StringBuilder sb = new StringBuilder();
            for (String job:intersection){
                sb.append(job).append(",");
            }
            //取不在推荐列表中的投递职位
            List<String> diffJobIDList = new ArrayList<String>();
            StringBuilder sb2 = new StringBuilder();
            for (String job:postJobIDList){
                if (!intersection.contains(job)){
                    diffJobIDList.add(job);
                    sb2.append(job).append(",");
                }
            }
            context.write(new Text(accountid), new Text(post_size+"\t"+rec_size+"\t"+inter_size+"\t"+
                    diffJobIDList.size()+"\t"+per+"\t"+sb.toString()+"\t"+sb2.toString()));
        }
    }

}
