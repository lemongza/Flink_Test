package com.example.flink.kafka;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

// Flink에서 정제된 데이터를 kafka로 내보낼 때 사용
// Sink를 생성하는 정적 메서드 제공
public class SinkProfile {

    // 지정한 토픽이 kafka에 존재하지 않으면 새로 생성
    public static void ensureTopicExists(String brokers, String topic, int partitions, short replicationFactor)
            throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        // AdminClient를 이용해 토픽 목록 확인 및 생성
        try (AdminClient admin = AdminClient.create(props)) {
            var topics = admin.listTopics().names().get();
            if (!topics.contains(topic)) {
                // 토픽이 없으면 새로 생성
                admin.createTopics(List.of(new NewTopic(topic, partitions, replicationFactor))).all().get();
                System.out.println("✅ [INFO] Created topic: " + topic);
            } else {
                System.out.println("✅ [INFO] Topic already exists: " + topic);
            }
        }
    }

    // 현재 채택한 방식
    // 값(value)만 String 직렬화 -> 카프카에 전송
    // key는 null 로 되어있음
    public static KafkaSink<String> createKafkaSink(String brokers, String topic) {
        // 토픽이 없으면 생성 시도 (실패해도 Job은 계속 진행)
        try {
            ensureTopicExists(brokers, topic, 3, (short) 1);
        } catch (Exception e) {
            System.err.println("[WARN] ensureTopicExists failed: " + e.getMessage());
            e.printStackTrace();
        }

        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers) // 브로커 주소 설정
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(topic) // Sink 토픽명
                                // String → UTF-8 byte[] 변환 (SimpleStringSchema와 동일한 기능)
                                .setValueSerializationSchema(s ->
                                        s == null ? null : s.getBytes(StandardCharsets.UTF_8))
                                .build()
                )
                .build();
    }

    // Key + value 모두 전송할 때 쓰는 Sink
    // Flink Tuple2<String, String>
    // f0 = key, f1 = value
    public static KafkaSink<org.apache.flink.api.java.tuple.Tuple2<String, String>> createKafkaSinkWithKey(
            String brokers, String topic) {

        // 토픽이 없으면 생성 시도
        try {
            ensureTopicExists(brokers, topic, 3, (short) 1);
        } catch (Exception e) {
            System.err.println("[WARN] ensureTopicExists failed: " + e.getMessage());
            e.printStackTrace();
        }

        return KafkaSink.<org.apache.flink.api.java.tuple.Tuple2<String, String>>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                //KafkaSink<Tuple2<String,String>>
                                // 각 스트림 요소 elem
                                // elem.f0 = key(이메일), elem.f1 = value (JSON 문자열)
                                .<org.apache.flink.api.java.tuple.Tuple2<String, String>>builder()
                                .setTopic(topic)
                                // key 직렬화 : Tuple2의 f0
                                .setKeySerializationSchema(elem -> {
                                    String k = elem == null ? null : elem.f0;
                                    return k == null ? null : k.getBytes(StandardCharsets.UTF_8);
                                })
                                // Value 직렬화 : Tuple2의 f1
                                .setValueSerializationSchema(elem -> {
                                    String v = elem == null ? null : elem.f1;
                                    return v == null ? null : v.getBytes(StandardCharsets.UTF_8);
                                })
                                .build()
                )
                .build();
    }
}