package org.example;


import org.example.db.hive.HiveConnector;

import java.sql.ResultSet;

public class Test {


    public static void main(String[] args)  {
        String s = "20220421";
        String mon = s.substring(0,s.length()-2);
        String path = "/hive/data/ext/dwd_log_job_rec_i_d/"+mon+"/"+s;
        System.out.println(path);
    }
}
