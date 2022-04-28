package org.example.percentcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class postrecPerCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // Hadoop会自动根据驱动程序的类路径来扫描该作业的Jar包。
        job.setJarByClass(org.example.percentcount.postrecPerCountDriver.class);

        // 指定mapper
        job.setMapperClass(postrecPerCountMapper.class);
        // 指定reducer
        job.setReducerClass(postrecPerCountReducer.class);

        // map程序的输出键-值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 输出键-值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入文件的路径
        String mon = args[0].substring(0,args[0].length()-2);
        FileInputFormat.setInputPaths(job, new Path("/hive/data/ext/dwd_log_job_post_rec_i_d/"+mon+"/"+args[0]));
        // 输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
        //mvn package -Dmaven.test.skip=true
        //mvn assembly:assembly -Dmaven.test.skip=true
        //hadoop jar bigdata-mr-1.0-SNAPSHOT.jar org.example.percentcount.postrecPerCountDriver 20220421 /mr/output/
    }
}
