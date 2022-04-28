package org.example.db.hive;

import java.sql.*;

public class HiveConnector {
    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String url = "jdbc:hive2://10.100.2.104:10000";
    private String dbName = "default";
    private static Connection conn = null;
    private static Statement state = null;
    private static ResultSet res = null;

    public void init(String dbName){
        this.dbName = dbName;
        init();
    }
    public void init(){
        try{
            Class.forName(driverName);
            conn = DriverManager.getConnection(url+"/"+dbName);
            state = conn.createStatement();
        }catch(ClassNotFoundException | SQLException e){
            e.printStackTrace();
        }
    }

    public void destory() throws SQLException {
        if(res != null) state.close();
        if(state != null) state.close();
        if(conn != null) conn.close();
    }

    public ResultSet showDB() throws SQLException {
        init();
        res = state.executeQuery("show databases");
        return res;
    }

    public void createDB(String dbName) throws SQLException {
        init();
        res = state.executeQuery("create database "+dbName);
    }

    public void dropDB(String dbName) throws ClassNotFoundException, SQLException {
        init();
        res = state.executeQuery("create database "+dbName);
    }

    public ResultSet showTabs(String dbName) throws SQLException {
        init(dbName);
        return state.executeQuery("show tables");
    }

    public void loadParData(String tableName,String partition,String path) throws SQLException {
        String sql = "ALTER TABLE "+dbName+"."+tableName+" ADD IF NOT EXISTS PARTITION ("+partition+") LOCATION '"+path+"'";
        state.execute(sql);
    }

    public void delPartion(String tableName,String partition) throws SQLException {
        String sql = "alter table "+dbName+"."+tableName+" drop partition("+partition+")";
        state.execute(sql);
    }

    public ResultSet showPartion(String tableName) throws SQLException {
        String sql = "show partitions "+dbName+"."+tableName;
        return state.executeQuery(sql);
    }
}
