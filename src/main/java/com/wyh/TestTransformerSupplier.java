package com.wyh;

import java.time.Duration;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.json.JSONObject;

public class TestTransformerSupplier implements TransformerSupplier<String, String, KeyValue<String, String>>{

	final private String stateStoreName;
	//初始化构造函数，把statestore传进来
	public TestTransformerSupplier(String stateStoreName) {
		this.stateStoreName = stateStoreName;
	}
	
	public Transformer<String, String, KeyValue<String, String>> get() {
		
		return new Transformer<String, String, KeyValue<String, String>>(){
			
			private KeyValueStore<String, String> testStateStore;
			private ProcessorContext context;
			
			@SuppressWarnings("unchecked")
			public void init(ProcessorContext context) {
				testStateStore = (KeyValueStore<String, String>) context.getStateStore(stateStoreName);
				this.context = context;
				this.context.schedule(Duration.ofMinutes(10), PunctuationType.STREAM_TIME, (timestamp) ->{
					KeyValueIterator<String, String> iterator = this.testStateStore.all();
					//遍历statestore中存储的每一个key-value实体
					while(iterator.hasNext()) {
						//这里即使你不对遍历出来的对象做什么操作，也需要执行这条语句，否则就会读不到数据
						KeyValue<String, String> object = iterator.next();
						//可以设置statestore中的数据过期时间或者其他的条件，可以执行删除,这里写的是删除操作，根据key删除，根据自己的业务加上删除的判断条件
						//testStateStore.delete(object.key);
					}
					iterator.close();
					context.commit();
				});
			}

			//这个方法中主要是写自己的业务逻辑
			//这里假设我要实现的是判断如果每个url执行了3次，那么就产生一条警告，并将这条警告放到要流出的topic中
			public KeyValue<String, String> transform(String key, String value) {
				String url = value;
				Integer count = null;
				//判断，先从statestore中获取这个url是不是已经存在，如果已经存在，就获取原来的count值，然后加1
				if(testStateStore.get(url) != null) {
					count = Integer.parseInt(testStateStore.get(url));
					count += 1;
					System.out.println(url+"执行次数:"+count);
					//将累加后的count重新放到statestore里面，就会采用最新值，参数一是存在statestore中的key，是自己指定的
					testStateStore.put(url, count.toString());//put()中的参数类型都是只能是string
					if(count >= 3) {//如果某个Url执行了3次，那么就产生一条消息，并将这个Url的count置0，重新计算
						testStateStore.put(url, "0");
						//返回的key,value就是要返回给调用该类的那个对象，也就是返回给TestStateStoreStream中调用者，然后再流出topic
						return KeyValue.pair(url+"-warning", count.toString());
					}
				}else {
					//该url未出现过，那么第一次来就把它置为1
					testStateStore.put(url, "1");
					//第一次出现，不符合我们想要达到的出现3次产生消息的需求，所以这里就返回null
					return null;
				}
				//其余条件全部返回Null
				return null;
			}

			public void close() {
				//此处我们可以不用操作，因为kafka streams会在必要的时候自动关闭statestore
			}
			
		};

	}

}
