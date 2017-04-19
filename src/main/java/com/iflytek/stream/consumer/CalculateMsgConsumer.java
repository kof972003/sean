package com.iflytek.stream.consumer;

import java.util.List;

import kafka.consumer.ConsumerIterator;

import org.apache.commons.lang.StringUtils;

import scala.actors.threadpool.Arrays;

import com.iflytek.stream.datamatch.DataMatch;
import com.iflytek.util.Constants;

public class CalculateMsgConsumer extends MessageConsumer {

	@Override
	public void consume() {
		ConsumerIterator<byte[], byte[]> consumerIter = getConsumerIter(Constants.CALCULATE_MSG_TOPIC);
		while(consumerIter.hasNext()){
//			System.out.println(new String(consumerIter.next().message()));
			String msgStr = new String(consumerIter.next().message());
			if(StringUtils.isEmpty(msgStr)){
				continue;
			}
			List<String> baseDataList = Arrays.asList(msgStr.split(Constants.SPLIT_COMMA));
			//计数
			DataMatch.matchCalData(baseDataList);
		}
	}

}
