package com.iflytek.test.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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
import org.apache.hadoop.hbase.util.Bytes;

import com.iflytek.util.Constants;

public class HbaseClient {

	public static Configuration configuration;
	private static final String TABLE_NAME = "PHOENIX_TEST2";
	
	static{
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master",Constants.HBASE_MASTER);
        configuration.set("hbase.zookeeper.quorum", Constants.ZK_ADDRESS_PORT) ;
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
	
	public static void insertData() throws Exception{
		Connection conn = ConnectionFactory.createConnection(configuration);
		Table table = conn.getTable(TableName.valueOf(TABLE_NAME));
		List<Put> putList = new ArrayList<Put>();
		Random random = new Random();
		for(int i = 1;i < 100;i++){
			/*String rowkey = i+"";
			if(StringUtils.isEmpty(rowkey)){
				continue;
			}
			Put put = new Put(rowkey.getBytes());*/
			Put put = new Put(Bytes.toBytes(i));
//			Put put = new Put(Bytes.toBytes(Bytes.toStringBinary(Bytes.toBytes(i))));
			put.addColumn(Bytes.toBytes("0"), Bytes.toBytes("NAME"), (TABLE_NAME + "_" + i).getBytes());
			put.addColumn(Bytes.toBytes("0"), Bytes.toBytes("AGE"), Bytes.toBytes(random.nextInt(18)));
			put.addColumn(Bytes.toBytes("0"), Bytes.toBytes("SCORE"), Bytes.toBytes(random.nextInt(100)));
			put.addColumn(Bytes.toBytes("0"), Bytes.toBytes("CLASSID"), Bytes.toBytes(random.nextInt(3)));
			putList.add(put);
		}
		System.out.println("data is saving");
		table.put(putList);
	}
	
	public static void main(String[] args){
		try {
			insertData();
//			System.out.println(new String(Bytes.toBytes(1)));
//			System.out.println(new String("1".getBytes()));
//			System.out.println(Bytes.toInt(Bytes.toBytesBinary("\\x80\\x00\\x00d")));
//			System.out.println(Bytes.toInt(Bytes.toBytes("\\x80\\x00\\x00d")));
//			System.out.println(Bytes.toStringBinary(Bytes.toBytes(1)));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static byte[] intToBytes(final int number) {  
		byte[] result = new byte[4];    
        //由高位到低位  
        result[0] = (byte)((number >> 24) & 0xFF);  
        result[1] = (byte)((number >> 16) & 0xFF);  
        result[2] = (byte)((number >> 8) & 0xFF);  
        result[3] = (byte)(number & 0xFF);  
        return result; 
    }  

}
