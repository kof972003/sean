package model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

public class UserInfo {
	
	private String rowkey;
	
	private String idStr;
	
	private Integer value;
	
	public UserInfo() {
		super();
	}
	
	public UserInfo(String idStr, Integer value) {
		this(StringUtils.isEmpty(idStr)?"":String.valueOf(idStr),idStr,value);
	}
	
	public UserInfo(String rowkey, String idStr, Integer value) {
		super();
		this.rowkey = rowkey;
		this.idStr = idStr;
		this.value = value;
	}

	public static UserInfo buildModel(String keyStr){
		if(StringUtils.isEmpty(keyStr) || keyStr.split("_").length != 2){
			return null;
		}
		String[] keyStrArr = keyStr.split("_");
		return new UserInfo(keyStrArr[0],Integer.valueOf(keyStrArr[1]));
	}
	
	public static List<UserInfo> buildModelList(List<String> dataList){
		if(CollectionUtils.isEmpty(dataList)){
			return Collections.EMPTY_LIST;
		}
		List<UserInfo> userInfoList = new ArrayList<UserInfo>();
		for(String dataStr : dataList){
			if(StringUtils.isEmpty(dataStr)){
				continue;
			}
			userInfoList.add(new UserInfo(dataStr,null));
		}
		return userInfoList;
	}
	
	public String getKey(){
		return (new StringBuilder(10).append(idStr).append("_").append(value)).toString();
	}
	
	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	public String getIdStr() {
		return idStr;
	}

	public void setIdStr(String idStr) {
		this.idStr = idStr;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

}
