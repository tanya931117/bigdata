package org.example.recfilter;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

public class RecFilterReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<String> recJobIDList = new ArrayList<String>();
        for(Text value: values){
            String[] recJobs = value.toString().split("\u0002");
            List<String> recJobIDListTMP = Arrays.asList(recJobs);
            recJobIDList.addAll(recJobIDListTMP);
        }
        //去重
        recJobIDList = new ArrayList<String>(new LinkedHashSet<>(recJobIDList));
        StringBuilder sb = new StringBuilder();
        for (int i = 0;i<recJobIDList.size();i++){
            sb.append(recJobIDList.get(i)).append("\u0002");
        }
        context.write(key, new Text(sb.toString().substring(0,sb.toString().length()-"\u0002".length())));
    }
}
