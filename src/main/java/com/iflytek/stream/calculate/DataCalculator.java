package com.iflytek.stream.calculate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import model.UserInfo;

import org.apache.commons.collections.CollectionUtils;

import com.iflytek.stream.schedule.CommandService;

public class DataCalculator implements CommandService {
	
	public static final int CUT_RANGE = 30;
	private static AtomicInteger batchNum = new AtomicInteger(1);
	private static Map<String,Map<String,Integer>> dataBatchMap = new ConcurrentHashMap<String,Map<String,Integer>>();
	
//	private static final int RETAIN_VERSION = 10;
	
	/**
	 * 更新数据批次
	 * @return
	 */
	public static int autoBatch(){
		return batchNum.incrementAndGet();
	}
	
	/*public static void addBatchMap(String batchNumStr,String columnStr,int incrNum){
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
	}*/
	
	/**
	 * 更新数据至当前批次
	 * @param columnStr
	 * @param incrNum
	 */
	public static void addBatchMap(String columnStr,int incrNum){
		String curVersionStr = String.valueOf(batchNum);
		Map<String,Integer> columnMap = null;
		//校验批次
		if(!dataBatchMap.containsKey(curVersionStr)){
			columnMap = new ConcurrentHashMap<String,Integer>();
			dataBatchMap.put(curVersionStr, columnMap);
		}else{
			columnMap = dataBatchMap.get(curVersionStr);
		}
		//校验匹配列
		if(!columnMap.containsKey(columnStr)){
			columnMap.put(columnStr, incrNum);
		}else{
			columnMap.put(columnStr, (columnMap.get(columnStr) + incrNum));
		}
	}
	
	/*public static void addBatchMapClean(String batchNumStr,String columnStr,int incrNum){
		addBatchMap(batchNumStr,columnStr,incrNum);
		reduceBatchMap();
	}*/
	
	/*public static void reduceBatchMap(){
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
	}*/
	
	public static Map<String,Map<String,Integer>> getBatchMap(){
		return dataBatchMap;
	}
	
	/**
	 * 根据制定step分割数据
	 * @param step
	 * @return
	 */
	public static Map<String,Map<String,Integer>> rebuildDataByStep(int step){
		int size = dataBatchMap.size();
		List<String> stepList = stepRange(step,CUT_RANGE,size);
		if(CollectionUtils.isEmpty(stepList)){
			return Collections.EMPTY_MAP;
		}
		Map<String,Map<String,Integer>> userResMap = new HashMap<String,Map<String,Integer>>();
		for(String stepStr : stepList){
			String[] arr = stepStr.split("_");
			int start = Integer.valueOf(arr[0]);
			int end = Integer.valueOf(arr[1]);
			Map<String,Integer> infoMap = new HashMap<String,Integer>();
			for(int index = start;index <= end;index++){
				Map<String,Integer> columnMap = dataBatchMap.get(String.valueOf(index));
				if(columnMap == null || columnMap.size() == 0){
					continue;
				}
				for(Entry<String,Integer> entry : columnMap.entrySet()){
					String idStr = entry.getKey();
					int num = entry.getValue() == null ? 0 : entry.getValue();
					if(infoMap.containsKey(idStr)){
						num = infoMap.get(idStr) + num;
					}
					infoMap.put(idStr, num);
				}
			}
			userResMap.put(stepStr, infoMap);
		}
		return userResMap;
	}
	
	/**
	 * 根据step分割生成指定区间
	 * @param step
	 * @param range
	 * @param size
	 * @return
	 */
	public static List<String> stepRange(int step,int range,int size){
		if(range > size){
			return Collections.EMPTY_LIST;
		}
		step = Math.min(Math.min(step, range), size);
		List<String> resultList = new ArrayList<String>();
		int start = size - range + 1;
		while(start < size){
			int end = start + step;
			resultList.add(start+"_"+end);
			start = end + 1;
		}
		return resultList;
	}
	
	@Override
	public void execute() {
		String curVersionStr = String.valueOf(batchNum);
		long start = System.currentTimeMillis();
		System.out.println("currentVersion:"+curVersionStr);
		System.out.println(start + "-----------");
		System.out.println(dataBatchMap);
		if(dataBatchMap == null  || dataBatchMap.size() == 0 ||
				!dataBatchMap.containsKey(curVersionStr)){
			return;
		}
		
		Map<String,Integer> columnMap = dataBatchMap.get(curVersionStr);
		if(columnMap == null || columnMap.size() == 0){
			return;
		}
		StringBuffer sBuf = new StringBuffer();
		sBuf.append("第").append(curVersionStr).append("秒：");
		for(Entry<String,Integer> entry : columnMap.entrySet()){
			sBuf.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
		}
		System.out.println(sBuf.toString());
		long end = System.currentTimeMillis();
		System.out.println(end + "-----------");
		System.out.println((end - start) + "========");
	}

}
