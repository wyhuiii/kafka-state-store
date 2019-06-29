package com.wyh;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class TestStateStoreStream {

	public static void main(String[] args) {
		
		//配置kafka服务信息
		Properties prop = new Properties();
		prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "wyh-stream-application");
		prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.184.128:9092");
		prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		prop.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\IT\\tool\\kafka-state-store");//设置状态仓库的存储路径
		
		//手动创建一个state store
		StoreBuilder<KeyValueStore<String, String>> testStateStore = Stores.keyValueStoreBuilder(
				//指定state-store的名字，运行时会自动在前面配置的STATE_DIR_CONFIG路径下创建该文件夹
				Stores.persistentKeyValueStore("wyh-state-store"),
				Serdes.String(),
				Serdes.String())
				.withCachingEnabled();
		StreamsBuilder builder = new StreamsBuilder();
		//向builder中添加state-store
		builder.addStateStore(testStateStore);
		//创建KStreams,从wyh-topic中读入流
		KStream<String, String> inputStream = builder.stream("wyh-topic-in");
		//使用transform实现自定义statestore,参数一是一个TransformerSupplier的实现类的对象，参数二是前面指定的state-store的名字
		KStream<String, String> transformStream = inputStream.transform(new TestTransformerSupplier(testStateStore.name()), testStateStore.name());
		//输出topic
		transformStream.to("wyh-topic-out");
		KafkaStreams streams = new KafkaStreams(builder.build(), prop);
		streams.start();
	}

}
