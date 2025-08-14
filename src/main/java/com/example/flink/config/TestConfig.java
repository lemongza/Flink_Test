package com.example.flink.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestConfig {

    private final Properties properties = new Properties();

    public TestConfig() {
        try (InputStream input = getClass()
                .getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new RuntimeException("config.properties 파일을 찾을 수 없습니다.");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("config.properties 로딩 실패", e);
        }
    }

    public String getBrokers() {
        return properties.getProperty("kafka.bootstrap.servers",
                properties.getProperty("kafka.brokers")); // fallback
    }

    public String getGroupId() {
        return properties.getProperty("kafka.group.id");
    }

    public String getSourceTopic() {
        return properties.getProperty("kafka.topic.source");
    }

    public String getSinkTopic() {
        return properties.getProperty("kafka.topic.sink");
    }

    public String getOffsetReset() {
        return properties.getProperty("kafka.offset.reset");
    }
}