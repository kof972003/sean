package com.iflytek.stream.persist;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import com.iflytek.util.Constants;

public class DataManager {
	
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
	
	public static void insertData(String tableName, List<String> dataList) throws Exception{
		if(StringUtils.isEmpty(tableName) || CollectionUtils.isEmpty(dataList)){
			System.out.println("table name is empty, or dataList is empty!");
			return;
		}
		Connection conn = ConnectionFactory.createConnection(configuration);
		Table table = conn.getTable(TableName.valueOf(tableName));
		List<Put> putList = new ArrayList<Put>();
		for(String str : dataList){
			Put put = new Put(str.getBytes());
			put.add("f1".getBytes(), "mark".getBytes(), "1".getBytes());
			putList.add(put);
		}
		System.out.println("data is saving");
		table.put(putList);
	}
	
	public static void insertDataWithReset(String tableName,List<String> dataList) throws Exception{
		creaTable(tableName);
		insertData(tableName, dataList);
	}
	
	public static void insertDataWithTableCheck(String tableNameStr,List<String> dataList) throws Exception{
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
        insertData(tableNameStr,dataList);
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
