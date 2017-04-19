package com.iflytek.stream.calculate;

import java.util.List;

import model.UserInfo;

import org.apache.commons.collections.CollectionUtils;

import com.iflytek.stream.datamatch.DataMatch;
import com.iflytek.stream.persist.CalculateMsgPersist;
import com.iflytek.stream.persist.DataManager;
import com.iflytek.stream.schedule.CommandService;
import com.iflytek.util.Constants;

public class CalDataStepStat implements CommandService {
	
	@Override
	public void execute() {
		List<UserInfo> userInfoList = DataMatch.buildUserInfoList(DataCalculator.rebuildDataByStep(5));
		if(CollectionUtils.isEmpty(userInfoList)){
			return;
		}
		DataManager dataManager = new CalculateMsgPersist();
		try {
			dataManager.insertDataWithTableCheck(Constants.CALCULATE_MSG_HTABLE, userInfoList);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
