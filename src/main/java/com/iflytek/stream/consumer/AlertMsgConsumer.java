package com.iflytek.stream.consumer;

import java.util.ArrayList;
import java.util.List;

import kafka.consumer.ConsumerIterator;
import model.UserInfo;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import scala.actors.threadpool.Arrays;

import com.iflytek.stream.datamatch.Creator;
import com.iflytek.stream.datamatch.DataMatch;
import com.iflytek.stream.persist.AlertMsgPersist;
import com.iflytek.stream.persist.DataManager;
import com.iflytek.util.Constants;

public class AlertMsgConsumer extends MessageConsumer {
	
	public void consume(){
		ConsumerIterator<byte[], byte[]> consumerIter = getConsumerIter(Constants.ALERT_MSG_TOPIC);
		List<String> dataList = new ArrayList<String>();
		List<String> speList = Creator.autoCreate();
		DataManager dataManager = new AlertMsgPersist();
		while(consumerIter.hasNext()){
//			System.out.println(new String(consumerIter.next().message()));
			String msgStr = new String(consumerIter.next().message());
			if(StringUtils.isEmpty(msgStr)){
				continue;
			}
			List<String> baseDataList = Arrays.asList(msgStr.split(Constants.SPLIT_COMMA));
			//数据匹配
			List<String> matchList = DataMatch.match(baseDataList, speList);
			if(CollectionUtils.isEmpty(matchList)){
				continue;
			}
			dataList.addAll(matchList);
			if(dataList.size() >= Constants.CONSUMER_COMMIT_SIZE){
				try {
					dataManager.insertDataWithTableCheck(Constants.ALERT_MSG_HTABLE,UserInfo.buildModelList(dataList));
					dataList.clear();
				} catch (Exception e) {
					System.out.println("insertDataWithTableCheck error : " + e.toString());
				}
			}
		}
		//补偿
		if(!CollectionUtils.isEmpty(dataList)){
			try {
				dataManager.insertDataWithTableCheck(Constants.ALERT_MSG_HTABLE,UserInfo.buildModelList(dataList));
				dataList.clear();
			} catch (Exception e) {
				System.out.println("insertDataWithTableCheck error");
			}
		}
	}

}
