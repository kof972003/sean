package com.iflytek.stream.datamatch;

import java.util.ArrayList;
import java.util.List;

public class Creator {
	
	public static final int NUM_RANGE = 100;
	public static final int NUM_SIZE = 40;
	public static final int CAL_LIMIT = 500;
	
	public static String autoCreateNum(int range){
		return String.valueOf((int)(Math.random() * NUM_RANGE));
	}
	
	public static List<String> autoCreate(){
		List<String> dataList = new ArrayList<String>();
		for(int i = 0;i < NUM_SIZE;i++){
			dataList.add(autoCreateNum(NUM_RANGE));
		}
		return dataList;
	}
	
	public static int autoCalLimit(){
		return CAL_LIMIT;
	}
	
}
