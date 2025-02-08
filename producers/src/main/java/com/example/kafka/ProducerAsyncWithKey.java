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

public class ProducerAsyncWithKey {
    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncWithKey.class.getName());
    public static void main(String[] args) {
        String topicName = "multipart-topic";

        // Kafkaproducer configuration setting
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer object create
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // ProducerRecord object create
        for (int seq = 0 ; seq < 20; seq++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "hello world"+seq);
            // KafkaProducer message send
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("\n ###### record metadata received ###### \n" +
                            "partition : " + metadata.partition() + "\n" +
                            "offset : " + metadata.offset() + "\n" +
                            "timestamp : " + metadata.timestamp());
                } else {
                    logger.error("exception error from broker" + exception.getMessage());
                }
            });
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
