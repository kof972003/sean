package com.iflytek.stream.calculate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DataCalculator {
	
	public static AtomicInteger batchNum = new AtomicInteger(1);
	public static Map<String,Map<String,Integer>> dataBatchMap = new ConcurrentHashMap<String,Map<String,Integer>>();
	
	public static final int RETAIN_VERSION = 10;
	
	public static int autoBatch(){
		return batchNum.incrementAndGet();
	}
	
	public static void addBatchMap(String batchNumStr,String columnStr,int incrNum){
		Map<String,Integer> columnMap = null;
		//校验批次
		if(!dataBatchMap.containsKey(batchNumStr)){
			columnMap = new ConcurrentHashMap<String,Integer>();
			dataBatchMap.put(batchNumStr, columnMap);
		}else{
			columnMap = dataBatchMap.get(batchNumStr);
		}
		//校验匹配列
		if(!columnMap.containsKey(columnStr)){
			columnMap.put(columnStr, incrNum);
		}else{
			columnMap.put(columnStr, (columnMap.get(columnStr) + incrNum));
		}
	}
	
	public static void addBatchMapClean(String batchNumStr,String columnStr,int incrNum){
		addBatchMap(batchNumStr,columnStr,incrNum);
		reduceBatchMap();
	}
	
	public static void reduceBatchMap(){
		//是否需进行冗余数据移除
		if(dataBatchMap.size() > RETAIN_VERSION){
			int removeFirst = batchNum.get() - dataBatchMap.size() + 1;
			int removeLast = batchNum.get() - RETAIN_VERSION + 1;
			for(int i = removeFirst;i < removeLast;i--){
				if(i < 0){
					continue;
				}
				dataBatchMap.remove(String.valueOf(i));
			}
		}
	}
	
	

}
