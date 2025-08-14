package com.example.flink.kafka;

import com.example.flink.model.RawProfile;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Kafka → DataStream<String> → RawProfile 매핑 유틸
 * - 같은 도커 네트워크에서 실행 시 brokers 예: "broker1:19092,broker2:19092,broker3:19092"
 * - 기본은 value-only(String JSON)로 소비합니다.
 */
public class SourceProfile {
    // Jackson ObjectMapper (알 수 없는 필드는 무시하도록 설정)
    // Jackson : Java 객체 <-> JSON 문자열 변환 도구
    // ObjectMapper는 설정 이후 쓰레드-세이프하므로 static 재사용이 일반적입니다.
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** KafkaSource<String> 생성 (value-only) */
    public static KafkaSource<String> kafkaSource(
            String brokers,
            String topic,
            String groupId,
            OffsetsInitializer startingOffsets
    ) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(brokers)                         // 브로커 주소 목록
                .setTopics(topic)                                     // 읽을 토픽
                .setGroupId(groupId)                                  // 컨슈머 그룹
                .setStartingOffsets(startingOffsets)                  // 시작 오프셋(earliest/latest)
                .setValueOnlyDeserializer(new SimpleStringSchema())   // value만 String으로 역직렬화
                .build();
    }

    /** Kafka → DataStream<String> (JSON 문자열 스트림) */
    public static DataStream<String> streamString(
            StreamExecutionEnvironment env,
            String brokers,
            String topic,
            String groupId,
            OffsetsInitializer startingOffsets
    ) {
        KafkaSource<String> source = kafkaSource(brokers, topic, groupId, startingOffsets);
        // 워터마크 없이 (이벤트 타임 미사용) 스트림 생성
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource(" + topic + ")")
                .returns(Types.STRING);
    }

    // 현재 채택하고 있는 방식
    /** Kafka → DataStream<RawProfile> (JSON → POJO 매핑) */
    public static DataStream<RawProfile> streamRawProfile(
            StreamExecutionEnvironment env,
            String brokers,
            String topic,
            String groupId,
            OffsetsInitializer startingOffsets
    ) {
        // JSON 문자열 -> RawProfile로 읽기
        return streamString(env, brokers, topic, groupId, startingOffsets)
                .map((MapFunction<String, RawProfile>) json -> MAPPER.readValue(json, RawProfile.class))
                .returns(RawProfile.class);
    }

    /** 편의 함수: earliest부터 RawProfile로 읽기 */
    public static DataStream<RawProfile> streamRawProfileEarliest(
            StreamExecutionEnvironment env,
            String brokers,
            String topic,
            String groupId
    ) {
        return streamRawProfile(env, brokers, topic, groupId, OffsetsInitializer.earliest());
    }
}