package com.iflytek.stream.producer;

import com.iflytek.util.Constants;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

public class AlertMsgProducer extends MessageProducer {
	
	public static final int NUM_RANGE = 1000;
	public static final int MSG_PER_LINE = 40;
//	public static final int MSG_COUNT = 3000;

	@Override
	protected void send(Producer<String, String> producer) {
		String msg = makeMsg(MSG_PER_LINE);
		if(msg == null || msg.trim().equals("")){
			return;
		}
		producer.send(new KeyedMessage(Constants.ALERT_MSG_TOPIC,msg));
	}
	
	public static String makeMsg(int msgCount){
		StringBuffer buf = new StringBuffer(msgCount);
		for(int i = 0;i < msgCount;i++){
			buf.append((int)(Math.random()*NUM_RANGE)).append(",");
		}
		return buf.toString();
	}

	/*public void sendMsg(String msg){
		if(msg == null || msg.trim().equals("")){
			return;
		}
		Producer producer = KafkaUtil.getProducer();
		producer.send(new KeyedMessage(TOPIC,msg));
		producer.close();
	}*/

}
