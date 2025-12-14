package com.consumer.gh_consumer.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticConfig {

    @Value("${elastic.url}")
    private String elasticUrl;

    @Bean(destroyMethod = "close")
    public RestClient restClient() {
        String noProto = elasticUrl.replace("http://", "").replace("https://", "");
        String[] hp = noProto.split(":");
        String host = hp[0];
        int port = Integer.parseInt(hp[1]);
        return RestClient.builder(new HttpHost(host, port, "http")).build();
    }

    @Bean
    public ElasticsearchTransport transport(RestClient restClient) {
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(ElasticsearchTransport transport) {
        return new ElasticsearchClient(transport);
    }
}
