package org.example.filterlog;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.utils.EnumUtil;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

public class filterLogReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // track:jobs_campus:navigation_1_click jsonstr
        String logtype = key.toString();
        try {
            if(EnumUtil.isInclude(LogType.class,logtype)){
                for (Text value :values){
                    context.write(new Text(""), value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
