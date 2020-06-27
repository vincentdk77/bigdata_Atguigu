package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class MyConsumer {
    public static void main(String[] args) {
        manualSyncCommit();
    }

    /**
     * 手动同步提交offset
     */
    public static void manualSyncCommit(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");//消费者组，只要 group.id 相同，就 属于同一个消费者组
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//自动提交 offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //offset重置方案
        //    当消费者更换了消费者组，或者消费者的offset数据已经过期删除（比如过了默认的7天时间kafka 的log数据已经被删除了）
        // earliest (默认)
        //    当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //    每个分区是从头开始消费的。
        // latest
        //    当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        //    表示消费新的数据（从consumer创建开始，后生产的数据），之前产生的数据不消费。
        // none
        //    topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("first"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            consumer.commitSync();
        }
    }
    /**
     * 手动异步提交offset （生产环境异步提交用的多）
     */
    public static void manualAsyncCommit(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");//消费者组，只要 group.id 相同，就 属于同一个消费者组
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//自动提交 offset
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("first"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            //异步提交
            consumer.commitAsync((Map< TopicPartition, OffsetAndMetadata > offsets, Exception exception) ->{
                if (exception != null) {
                    System.err.println("Commit failed for" +offsets);
                }
            });
        }
    }
    /**
     * 自动提交offset
     */
    public static void autoCommit(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");//消费者组，只要 group.id 相同，就 属于同一个消费者组
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");//自动提交 offset
        //自动提交延时
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //offset重置方案
        //    当消费者更换了消费者组，或者消费者的offset数据已经过期删除（比如过了默认的7天时间kafka 的log数据已经被删除了）
        // earliest (默认)
        //    当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
        //    每个分区是从头开始消费的。
        // latest
        //    当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
        //    表示消费新的数据（从consumer创建开始，后生产的数据），之前产生的数据不消费。
        // none
        //    topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("first"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            consumer.commitSync();
        }
    }

    /**
     * 自定义存储offset
     */
    public static void customSaveOffset(){
        Map<TopicPartition, Long> currentOffset = new HashMap<>();
        //创建配置信息
        Properties props = new Properties();
        //Kafka 集群
        props.put("bootstrap.servers", "hadoop102:9092");
        //消费者组，只要 group.id 相同，就属于同一个消费者组
        props.put("group.id", "test");
        //关闭自动提交 offset
        props.put("enable.auto.commit", "false");
        //Key 和 Value 的反序列化类
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //创建一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //消费者订阅主题
        consumer.subscribe(Arrays.asList("first"), new ConsumerRebalanceListener() {
                    //该方法会在 Rebalance 之前调用
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        commitOffset(currentOffset);
                    }

                    //该方法会在 Rebalance 之后调用
                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        currentOffset.clear();
                        for (TopicPartition partition : partitions) {
                            consumer.seek(partition, getOffset(partition));//定位到最近提交的 offset 位置继续消费
                        }
                    }
                });
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//消费者拉取数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
            commitOffset(currentOffset);//异步提交
        }
    }
    //获取某分区的最新 offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }
    //提交该消费者所有分区的 offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }
}
