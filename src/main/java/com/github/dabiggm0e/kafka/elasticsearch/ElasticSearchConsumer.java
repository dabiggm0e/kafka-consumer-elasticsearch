package com.github.dabiggm0e.kafka.elasticsearch;

import com.github.dabiggm0e.kafka.KafkaConsumerClient;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.client.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class ElasticSearchConsumer {

    private static JsonParser jsonParser = new JsonParser();

    static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    public static String extractIdFromTweet(String jsonRequest) {

        String id = jsonParser.parse(jsonRequest)
                              .getAsJsonObject()
                              .get("id_str")
                              .getAsString();
        return id;
    }

    public static void main(String[] args) throws IOException {

        String bootstrapServers = "localhost:9092";
        String groupId = "my-fifth-application";
        String topic = "twitter-tweets";
        int duration = 100;

        KafkaConsumerClient kafkaConsumerClient = new KafkaConsumerClient(
                bootstrapServers,
                groupId,
                topic
        );

        KafkaConsumer<String, String> kafkaConsumer = kafkaConsumerClient.createConsumer();


        String hostname = "localhost";
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);



        // poll data
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(duration));
            for (ConsumerRecord<String, String> record: records) {
                logger.info("Topic: " + record.topic() + ", Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                String jsonIdString = extractIdFromTweet(record.value());
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        jsonIdString // added key for idempotency
                ).source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String id = indexResponse.getId();

                logger.info("JSON id: " + jsonIdString + ". " + id);

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }

        //client.close();

    }
}
