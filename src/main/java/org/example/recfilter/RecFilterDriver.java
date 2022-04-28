package org.example.recfilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.db.hive.HiveConnector;

public class RecFilterDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // Hadoop会自动根据驱动程序的类路径来扫描该作业的Jar包。
        job.setJarByClass(RecFilterDriver.class);

        // 指定mapper
        job.setMapperClass(RecFilterMapper.class);
        // 指定reducer
        job.setReducerClass(RecFilterReducer.class);

        // map程序的输出键-值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 输出键-值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入文件的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 输出文件路径
        //20220421
        String mon = args[1].substring(0,args[1].length()-2);
        String path = "/hive/data/ext/dwd_log_job_rec_i_d/"+mon+"/"+args[1];
        FileOutputFormat.setOutputPath(job, new Path(path));

        boolean res = job.waitForCompletion(true);
        if(res){
            //添加hive分区
            HiveConnector connector = new HiveConnector();
            try{
                connector.init("hive");
                ///hive/data/ext/dwd_log_job_rec_i_d/202204/20220421
                String par = "mon="+mon+",dt="+args[1];
                connector.loadParData("dwd_log_job_rec_i_d",par,path);
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                try {
                    connector.destory();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
        System.exit(res?0:1);
        //hadoop jar bigdata-mr-1.0-SNAPSHOT-jar-with-dependencies.jar org.example.recfilter.RecFilterDriver /mr/input 20220421
    }
}
