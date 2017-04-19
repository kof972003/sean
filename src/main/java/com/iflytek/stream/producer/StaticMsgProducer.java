package com.iflytek.stream.producer;

import com.iflytek.util.Constants;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

public class StaticMsgProducer extends MessageProducer {
	
	public static final int NUM_RANGE = 1000;
	public static final int MSG_PER_LINE = 40;

	@Override
	protected void send(Producer<String, String> producer) {
		String msg = makeMsg(MSG_PER_LINE);
		if(msg == null || msg.trim().equals("")){
			return;
		}
		producer.send(new KeyedMessage(Constants.STATIC_MSG_TOPIC,msg));
	}
	
	public static String makeMsg(int msgCount){
		StringBuffer buf = new StringBuffer(msgCount);
		for(int i = 0;i < msgCount;i++){
			buf.append((int)(Math.random()*NUM_RANGE)).append(",");
		}
		return buf.toString();
	}

}
