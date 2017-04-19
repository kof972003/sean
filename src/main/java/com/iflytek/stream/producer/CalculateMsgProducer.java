package com.iflytek.stream.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import com.iflytek.util.Constants;

public class CalculateMsgProducer extends MessageProducer {
	
	public static final int ID_RANGE = 10;
	public static final int SCORE_RANGE = 100;
	public static final int MSG_PER_LINE = 5000;
//	public static final int MSG_COUNT = 3000;

	@Override
	protected void send(Producer<String, String> producer) {
		String msg = makeMsg(MSG_PER_LINE);
		if(msg == null || msg.trim().equals("")){
			return;
		}
		producer.send(new KeyedMessage(Constants.CALCULATE_MSG_TOPIC,msg));
	}
	
	public static String makeMsg(int msgCount){
		StringBuffer buf = new StringBuffer(msgCount);
		for(int i = 0;i < msgCount;i++){
			buf.append((int)(Math.random()*ID_RANGE)).append("_").append((int)(Math.random()*SCORE_RANGE)).append(",");
		}
		return buf.toString();
	}

}
