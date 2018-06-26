package com.dkl.leanring.spark.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import kafka.message.Message;

public class Rest {

	private static String seek(String[] args) {
		String topic = args[2];
		int partition = Integer.parseInt(args[3]);
		int offset = Integer.parseInt(args[4]);

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "group1");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "com.___.kafka.message.MessageValueDeserializer");
		KafkaConsumer<String, Message> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
		//consumer.subscribe(Arrays.asList(topic)); //"deviceInfoTopic"  
		TopicPartition topicPartition = new TopicPartition(topic, partition);
		consumer.assign(Arrays.asList(topicPartition));

		consumer.seek(new TopicPartition(topic, partition), offset);
		consumer.close();
		return "SUCC";
	}

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "ambari.master.com:6667");
		props.put("group.id", "KafkaOffsetDemo");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

		TopicPartition partition0 = new TopicPartition("top1", 0);

		consumer.assign(Arrays.asList(partition0));

		//		consumer.seek(partition0, 220);
		//		consumer.seekToEnd(Arrays.asList(partition0));
		consumer.seekToBeginning(Arrays.asList(partition0));

	}

}
