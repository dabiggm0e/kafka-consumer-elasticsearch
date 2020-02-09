package com.github.dabiggm0e.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;



public class KafkaConsumerClient {
    static Logger logger = LoggerFactory.getLogger(KafkaConsumerClient.class.getName());
    String bootstrapServers;
    String groupId;
    String topic;

    public KafkaConsumerClient( String bootstrapServers,
                                String groupId,
                                String topic) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;

    }


    public KafkaConsumer<String, String> createConsumer() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  //disable auto commit of offset
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to topic(s)
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) {

/*

        // poll data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(duration));
            for (ConsumerRecord<String, String> record: records) {
                logger.info("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }

*/
    }
}
