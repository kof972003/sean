package com.iflytek.stream.persist;

import java.util.ArrayList;
import java.util.List;

import model.UserInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class CalculateMsgPersist extends DataManager {
	
	@Override
	public void insertData(String tableName, List<UserInfo> dataList)
			throws Exception {
		if(StringUtils.isEmpty(tableName) || CollectionUtils.isEmpty(dataList)){
			System.out.println("table name is empty, or dataList is empty!");
			return;
		}
		Connection conn = ConnectionFactory.createConnection(configuration);
		Table table = conn.getTable(TableName.valueOf(tableName));
		List<Put> putList = new ArrayList<Put>();
		for(UserInfo userInfo : dataList){
			String rowkey = userInfo.getRowkey();
			if(StringUtils.isEmpty(rowkey)){
				continue;
			}
			Put put = new Put(rowkey.getBytes());
			put.add("f1".getBytes(), "id".getBytes(), StringUtils.isEmpty(userInfo.getIdStr()) ? "".getBytes() : userInfo.getIdStr().getBytes());
			put.add("f1".getBytes(), "value".getBytes(), userInfo.getValue() == null ? "".getBytes() : (String.valueOf(userInfo.getValue())).getBytes());
			putList.add(put);
		}
		System.out.println("data is saving");
		table.put(putList);

	}

}
