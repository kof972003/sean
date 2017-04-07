package com.iflytek.stream.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.iflytek.util.Constants;

public abstract class MessageConsumer {
	
	private ConsumerConnector consumer;
	
	public MessageConsumer(){
		init();
	}
	
	public ConsumerIterator<byte[],byte[]> getConsumerIter(String topic){
		Map<String,Integer> topickMap = new HashMap<String, Integer>();   
        topickMap.put(topic, 1);   
        Map<String, List<KafkaStream<byte[],byte[]>>>  streamMap =consumer.createMessageStreams(topickMap);   
        
        KafkaStream<byte[],byte[]>stream = streamMap.get(topic).get(0);   
        return stream.iterator();   
	}
	
	public abstract void consume();
	
	private void init(){
		consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
	}
	
	private static ConsumerConfig createConsumerConfig() {   
		Properties props = new Properties();   
        //配置要连接的zookeeper地址与端口
        //The ‘zookeeper.connect’ string identifies where to find once instance of Zookeeper in your cluster.
        //Kafka uses ZooKeeper to store offsets of messages consumed for a specific topic and partition by this Consumer Group
        props.put("zookeeper.connect",Constants.ZK_ADDRESS_PORT);
        
        //配置zookeeper的组id (The ‘group.id’ string defines the Consumer Group this process is consuming on behalf of.)
        props.put("group.id", "0");
        
        //配置zookeeper连接超时间隔
        //The ‘zookeeper.session.timeout.ms’ is how many milliseconds Kafka will wait for 
        //ZooKeeper to respond to a request (read or write) before giving up and continuing to consume messages.
        props.put("zookeeper.session.timeout.ms","5000"); 
 
        //The ‘zookeeper.sync.time.ms’ is the number of milliseconds a ZooKeeper ‘follower’ can be behind the master before an error occurs.
        props.put("zookeeper.sync.time.ms", "200");

        //The ‘auto.commit.interval.ms’ setting is how often updates to the consumed offsets are written to ZooKeeper. 
        //Note that since the commit frequency is time based instead of # of messages consumed, if an error occurs between updates to ZooKeeper on restart you will get replayed messages.
        props.put("auto.commit.interval.ms", "1000");
        props.put("rebalance.max.retries", "5");  
        props.put("rebalance.backoff.ms", "1200"); 
        
        return new ConsumerConfig(props);   
    }   

}
