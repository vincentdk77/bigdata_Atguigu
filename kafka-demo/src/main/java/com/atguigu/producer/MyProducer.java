package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MyProducer {
    public static void main(String[] args) {
        withNoCallFunctionProducer();
        withCallFunctionProducer();
        withNoCallFunctionAndPartitionerProducer();
    }

    /**
     * 异步发送
     * 不带回调函数的生产者
     */
    public static void withNoCallFunctionProducer(){
        Properties props = new Properties();
        //kafka 集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "foo-1:9092,foo-2:9092,foo-3:9092,foo-4:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //批次大小 (多大发一次，发的数据会存入缓冲区中，待Sender 线程去取数据发送给kafka)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //RecordAccumulator 缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i)));
        }
        producer.close();
    }

    /**
     * 异步发送
     * 带回调函数的生产者
     */
    public static void withCallFunctionProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "foo-1:9092,foo-2:9092,foo-3:9092,foo-4:9092");//kafka 集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小(多大发一次，发的数据会存入缓冲区中，待Sender 线程去取数据发送给kafka)
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator 缓冲区大小
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new
                KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            //使用匿名内部类
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i)), new Callback() {
                //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                @Override
                public void onCompletion(RecordMetadata metadata,Exception exception) {
                    //exception=null 表示一切正常
                    if (exception == null) {
                        System.out.println("partition->"+metadata.partition()+"offset->" +metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
            //使用lambda表达式
//            producer.send(new ProducerRecord<String, String>("first",Integer.toString(i), Integer.toString(i)), (metadata,exception) -> {
//                if (exception == null) {
//                    System.out.println("partition->"+metadata.partition()+"offset->" +metadata.offset());
//                } else {
//                    exception.printStackTrace();
//                }
//            });
        }
        producer.close();
    }

    /**
     * 异步发送
     * 不带回调函数的生产者
     * 指定分区器
     */
    public static void withNoCallFunctionAndPartitionerProducer(){
        Properties props = new Properties();
        //kafka 集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "foo-1:9092,foo-2:9092,foo-3:9092,foo-4:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //批次大小 (多大发一次，发的数据会存入缓冲区中，待Sender 线程去取数据发送给kafka)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //RecordAccumulator 缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //指定分区
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.atguigu.partitioner.MyPartitioner");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", Integer.toString(i)));
        }
        producer.close();
    }

    /**
     * 同步发送
     * 同步发送的意思就是，一条消息发送之后，会阻塞当前线程，直至返回 ack。
     * 由于 send 方法返回的是一个 Future 对象，根据 Futrue 对象的特点，我们也可以实现同 步发送的效果，只需在调用 Future 对象的 get 方发即可。
     */
    public static void syncSendProducer(){
        Properties props = new Properties();
        //kafka 集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "foo-1:9092,foo-2:9092,foo-3:9092,foo-4:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //批次大小 (多大发一次，发的数据会存入缓冲区中，待Sender 线程去取数据发送给kafka)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //RecordAccumulator 缓冲区大小
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            Future<RecordMetadata> first = producer.send(new ProducerRecord<String, String>("first", Integer.toString(i)));
            try {
                RecordMetadata recordMetadata = first.get();
                int partition = recordMetadata.partition();
                long offset = recordMetadata.offset();
                System.out.println("partition->"+partition+"offset->" +offset);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
    }
}
