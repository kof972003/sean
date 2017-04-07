package com.iflytek.stream.datamatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;

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

}
