package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncCustomCallBack {
    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCallBack.class.getName());
    public static void main(String[] args) {
        String topicName = "multipart-topic";

        // Kafkaproducer configuration setting
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object create
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord object create
        for (int seq = 0 ; seq < 20; seq++) {
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world"+seq);
            Callback customCallback = new CustomCallback(seq);

            // KafkaProducer message send
            kafkaProducer.send(producerRecord, customCallback);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // batch로 수행되는것이라 바로 메시지 전송을 위함
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
