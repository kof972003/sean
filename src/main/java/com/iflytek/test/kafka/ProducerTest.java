package com.iflytek.test.kafka;

import java.util.Properties;

import com.iflytek.util.Constants;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerTest {

	public static void main(String[] args) {
		Properties props = new Properties();

//		props.put("zk.connect", "192.168.59.22:2181");

		props.put("metadata.broker.list",
				Constants.KAFKA_BROKER_LIST);

		props.put("serializer.class", "kafka.serializer.StringEncoder");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		// 发送业务消息

		// 读取文件 读取内存数据库 读socket端口

		/*for (int i = 1; i <= 100; i++) {

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			producer.send(new KeyedMessage<String, String>("test_xlq",

			"i said i love you baby for" + i
					+ "times,will you have a nice day with me tomorrow"));

		}*/
		producer.send(new KeyedMessage<String, String>("test_xlq","test22111"));
	}

}
