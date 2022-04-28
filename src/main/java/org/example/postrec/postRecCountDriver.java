package org.example.postrec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.db.hive.HiveConnector;

public class postRecCountDriver {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // Hadoop会自动根据驱动程序的类路径来扫描该作业的Jar包。
        job.setJarByClass(postRecCountDriver.class);

        // 指定mapper
        job.setMapperClass(postRecCountMapper.class);
        // 指定reducer
        job.setReducerClass(postRecCountReducer.class);

        // map程序的输出键-值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 输出键-值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 输入文件的路径
        //20220421
        String mon = args[0].substring(0,args[0].length()-2);
        FileInputFormat.setInputPaths(job, new Path("/hive/data/ext/dwd_log_job_post_i_d/"+mon+"/"+args[0]),
                new Path("/hive/data/ext/dwd_log_job_rec_i_d/"+mon+"/"+args[0]));
        /// 输出文件路径
        String path = "/hive/data/ext/dwd_log_job_post_rec_i_d/"+mon+"/"+args[0];
        FileOutputFormat.setOutputPath(job, new Path(path));

        boolean res = job.waitForCompletion(true);

        if(res){
            //添加hive分区
            HiveConnector connector = new HiveConnector();
            try{
                connector.init("hive");
                ///hive/data/ext/dwd_log_job_post_rec_i_d/202204/20220421
                String par = "mon="+mon+",dt="+args[0];
                connector.loadParData("dwd_log_job_post_rec_i_d",par,path);
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
        //hadoop jar bigdata-mr-1.0-SNAPSHOT.jar org.example.postrec.postRecCountDriver 20220421
    }
}
