package com.github.dabiggm0e.kafka.elasticsearch;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.client.*;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchConsumer {

    private static JsonParser jsonParser = new JsonParser();

    public static String extractIdFromTweet(String jsonRequest) {
        String id = "";
       // jsonParser.parse(jsonRequest)
         //       .getAsString().
        return id;
    }

    public static void main(String[] args) {
        String hostname = "localhost";
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);

        String jsonString = "{ \"foo\": \"bar\" }";
    }
}
