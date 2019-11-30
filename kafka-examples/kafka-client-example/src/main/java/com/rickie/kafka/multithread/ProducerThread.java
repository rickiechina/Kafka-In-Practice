package com.rickie.kafka.multithread;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerThread extends Thread {
    private final Logger LOGGER = LoggerFactory.getLogger(ProducerThread.class);
    private final static int MAX_THREAD_SIZE = 3;

    /**
     * 配置Kafka Producer配置信息
     * @return
     */
    public Properties configure() {
        Properties props = new Properties();
        // 设置kafka 集群地址
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 设置应答机制
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        // 设置批量提交大小
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        // 延时提交
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // 缓冲大小
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
        // 序列化key
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        // 序列化Value
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        return props;
    }

    @Override
    public void run() {
        Producer<String, String> producer = new KafkaProducer<String, String>(configure());
        String topic = "test_topic";
        // 发送消息
        for(int i=0; i<10; i++) {
            JSONObject json = new JSONObject();
            json.put("id", i);
            json.put("username", "Jacky-" + i);
            json.put("age", i);
            json.put("date", new Date().toString());
            String key = "key" + i;

            // 异步发送，调用callback函数
            producer.send(new ProducerRecord<String, String>(
                            topic, key, json.toJSONString()), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e!= null) {
                                LOGGER.error("发送错误... 异常信息：" + e.getMessage());
                            } else {
                                LOGGER.info("线程：" + Thread.currentThread().getName() +
                                        " Topic: " + recordMetadata.topic() +
                                        " Partition: " + recordMetadata.partition() +
                                        " Offset: " + recordMetadata.offset());
                            }
                        }
                    }
            );

            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // 间隔3秒
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 关闭生产者对象
        producer.close();
    }

    public static void main(String[] args) {
        // 创建一个固定线程数量的线程池
        ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREAD_SIZE);
        for(int i=0; i<3; i++) {
            // 提交任务
            executorService.submit(new ProducerThread());
        }
        // 关闭线程池
        executorService.shutdown();
    }
}
