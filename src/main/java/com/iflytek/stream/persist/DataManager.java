package com.iflytek.stream.persist;

import java.util.List;

import model.UserInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.iflytek.util.Constants;

public abstract class DataManager {
	
	public static Configuration configuration;
	
	static{
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master",Constants.HBASE_MASTER);
        configuration.set("hbase.zookeeper.quorum", Constants.ZK_ADDRESS_PORT) ;
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");  
//        configuration.set("hbase.zookeeper.quorum", "192.168.59.22"); 
    }
	
	public static void creaTable(String tableNameStr) throws Exception{
//		HConnection connection = HConnectionManager.createConnection(configuration);
		Connection conn = ConnectionFactory.createConnection(configuration);
		Admin admin = conn.getAdmin();
//		HBaseAdmin admin = new HBaseAdmin(configuration);
		TableName tableName = TableName.valueOf(tableNameStr);
        if(admin.tableExists(tableName)){
        	System.out.println("table is exist,dropping");
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
//        System.out.println("新的表正在创建中！！！");
        System.out.println("table is creating");
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("f1"));
        admin.createTable(tableDescriptor);
    }
	
	public abstract void insertData(String tableName, List<UserInfo> userInfoList) throws Exception;
	
	public void insertDataWithTimeCost(String tableName, List<UserInfo> userInfoList) throws Exception{
		long start = System.currentTimeMillis();
		insertData(tableName, userInfoList);
		long end = System.currentTimeMillis();
		long cost = end - start;
		System.out.println("处理数据条数：" + userInfoList.size() + "条，总共耗时："+cost+"毫秒，平均每条数据耗时：" + cost/userInfoList.size()+"毫秒");
	}
	
	public void insertDataWithReset(String tableName,List<UserInfo> userInfoList) throws Exception{
		creaTable(tableName);
		insertDataWithTimeCost(tableName, userInfoList);
	}
	
	public void insertDataWithTableCheck(String tableNameStr,List<UserInfo> userInfoList) throws Exception{
		Connection conn = ConnectionFactory.createConnection(configuration);
		Admin admin = conn.getAdmin();
//		HBaseAdmin admin = new HBaseAdmin(configuration);
		TableName tableName = TableName.valueOf(tableNameStr);
        if(!admin.tableExists(tableName)){
        	HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor("f1"));
            admin.createTable(tableDescriptor);
            System.out.println("table is created");
        }
        insertDataWithTimeCost(tableNameStr,userInfoList);
	}
	
	public static void main(String[] args){
		try {
			creaTable("test_xlq");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
