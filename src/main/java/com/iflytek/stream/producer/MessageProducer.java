package com.iflytek.stream.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import com.iflytek.stream.schedule.CommandService;
import com.iflytek.util.Constants;

public abstract class MessageProducer implements CommandService {
	
	private Producer<String,String> producer;
	
	public MessageProducer(){
		init();
	}
	
	public void sendMsg(){
		send(producer);
//		close();
	}
	
	protected abstract void send(Producer<String,String> producer);
	
	@Override
	public void execute() {
		sendMsg();
	}
	
	private void init(){
		Properties props = new Properties();
		props.put("metadata.broker.list",Constants.KAFKA_BROKER_LIST);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}
	
	private void close(){
		if(producer != null){
			producer.close();
		}
	}

}
