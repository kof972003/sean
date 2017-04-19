package com.iflytek.stream.datamatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import model.UserInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import com.iflytek.stream.calculate.DataCalculator;

public class DataMatch {
	
	public static List<String> match(List<String> baseDataList,List<String> speList){
		if(CollectionUtils.isEmpty(baseDataList) || CollectionUtils.isEmpty(speList)){
			return Collections.emptyList();
		}
		List<String> matchList = new ArrayList<String>();
		for(String baseDataStr : baseDataList){
			if(speList.contains(baseDataStr)){
				matchList.add(baseDataStr);
			}
		}
		return matchList;
	}
	
	public static void matchCalData(List<String> baseDataList){
		if(CollectionUtils.isEmpty(baseDataList)){
			return;
		}
		for(String keyStr : baseDataList){
			UserInfo userInfo = UserInfo.buildModel(keyStr);
			if(userInfo == null){
				continue;
			}
			DataCalculator.addBatchMap(userInfo.getIdStr(), userInfo.getValue());
		}
	}
	
	public static Map<String,Map<String,Integer>> buildCalResults(Map<String,Map<String,Integer>> calDataMap){
		if(calDataMap == null || calDataMap.size() == 0){
			return Collections.EMPTY_MAP;
		}
		Map<String,Map<String,Integer>> resultMap = new HashMap<String,Map<String,Integer>>();
		for(Entry<String,Map<String,Integer>> entry : calDataMap.entrySet()){
			String key = entry.getKey();
			Map<String,Integer> columnMap = entry.getValue();
			if(columnMap == null || columnMap.size() == 0){
				continue;
			}
			Map<String,Integer> resultColumnMap = null;
			for(Entry<String,Integer> columnEntry : columnMap.entrySet()){
				String columnKey = columnEntry.getKey();
				Integer columnValue = columnEntry.getValue();
				//判断是否超过阈值
				if(columnValue != null && columnValue.intValue() > Creator.autoCalLimit()){
					if(resultColumnMap == null){
						resultColumnMap = new HashMap<String,Integer>();
					}
					resultColumnMap.put(columnKey, columnValue);
				}
			}
			if(resultColumnMap != null && resultColumnMap.size() > 0){
				resultMap.put(key, resultColumnMap);
			}
		}
		return resultMap;
	}
	
	public static List<UserInfo> buildUserInfoList(Map<String,Map<String,Integer>> calDataMap){
		if(calDataMap == null || calDataMap.size() == 0){
			return Collections.EMPTY_LIST;
		}
		List<UserInfo> userInfoList = new ArrayList<UserInfo>();
		for(Entry<String,Map<String,Integer>> entry : calDataMap.entrySet()){
			String key = entry.getKey();
			Map<String,Integer> columnMap = entry.getValue();
			if(columnMap == null || columnMap.size() == 0){
				continue;
			}
			Map<String,Integer> resultColumnMap = null;
			for(Entry<String,Integer> columnEntry : columnMap.entrySet()){
				String columnKey = columnEntry.getKey();
				Integer columnValue = columnEntry.getValue();
				//判断是否超过阈值
				if(columnValue != null && columnValue.intValue() > Creator.autoCalLimit()){
					userInfoList.add(new UserInfo(key+"_"+columnKey,columnKey,columnValue));
				}
			}
		}
		return userInfoList;
	}

}
