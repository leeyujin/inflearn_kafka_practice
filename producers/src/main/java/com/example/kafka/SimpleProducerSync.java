package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class.getName());
    public static void main(String[] args) {
        String topicName = "simple-topic";

        // Kafkaproducer configuration setting
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord object create
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world2");

        // KafkaProducer message send
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n ###### record metadata received ###### \n" +
                        "partition : " + recordMetadata.partition() + "\n" +
                        "offset : " + recordMetadata.offset() + "\n" +
                        "timestamp : " + recordMetadata.timestamp());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } finally {
            kafkaProducer.close();
        }

        // batch로 수행되는것이라 바로 메시지 전송을 위함
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
