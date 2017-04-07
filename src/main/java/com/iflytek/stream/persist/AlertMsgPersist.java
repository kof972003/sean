/*package com.iflytek.stream.persist;

import java.util.List;

public class AlertMsgPersist {
	
	public static void resetTable(String tableName){
		try {
			DataManager.creaTable(tableName);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("reset table error");
		}
	}
	
	public static void insertData(String tableName,List<String> dataList){
		try {
			DataManager.insertData(tableName, dataList);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("insert data error");
		}
	}
	
	

}
*/