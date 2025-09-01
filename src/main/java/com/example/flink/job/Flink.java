package com.example.flink.job;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class Flink {

    // ===== CloudEvent 스키마 =====
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CloudEvent implements Serializable {
        public String specversion;
        public String id;
        public String source;
        public String type;
        public String datacontenttype;
        public String subject;
        public String time;                 // e.g. 2025-08-26T08:17:59.188027493Z (UTC)
        public String route;
        public EventData data;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EventData implements Serializable {
        public String serviceName;
        public String methodName;
        public String resourceName;
        public AuthenticationInfo authenticationInfo;
        public AuthorizationInfo authorizationInfo;
        public Request request;
        public RequestMetadata requestMetadata;
        public List<ClientAddress> clientAddress;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AuthenticationInfo implements Serializable {
        public String principal;
        public String principalResourceId;
        public String identity;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AuthorizationInfo implements Serializable {
        public boolean granted;
        public String operation;            // e.g. DescribeConfigs
        public String resourceType;         // e.g. Topic
        public String resourceName;         // e.g. profiles.raw
        public String patternType;          // e.g. LITERAL
        public Boolean superUserAuthorization;
        public List<String> assignedPrincipals;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Request implements Serializable {
        public String correlation_id;
        public String client_id;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RequestMetadata implements Serializable {
        public String request_id;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ClientAddress implements Serializable {
        public String ip;
        public Integer port;
        public Boolean internal;
    }

    // ===== Sink에 보낼 평탄화된 레코드 =====
    public static class ParsedLog implements Serializable {
        public boolean granted;
        public String eventTimeUtc;         // 원본 time(UTC)
        public String eventTimeSeoul;       // Asia/Seoul로 변환
        public String principal;
        public String clientIp;
        public Integer clientPort;
        public Boolean clientInternal;
        public String methodName;
        public String resourceType;
        public String dataResourceName;     // data.resourceName
        public String authResourceName;     // authorizationInfo.resourceName
        public String operation;            // authorizationInfo.operation
        public String patternType;          // authorizationInfo.patternType
        public String requestId;            // requestMetadata.request_id
        public String correlationId;        // request.correlation_id
        public String clientId;             // request.client_id
        public long processingTime;
    }

    public static class ParsedLogSerializer implements SerializationSchema<ParsedLog> {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        @Override public byte[] serialize(ParsedLog element) {
            try { return MAPPER.writeValueAsBytes(element); }
            catch (Exception e) { throw new RuntimeException("JSON serialization failed", e); }
        }
    }

    private static String toSeoul(String utcIso) {
        if (utcIso == null || utcIso.isEmpty()) return null;
        try {
            Instant instant = Instant.parse(utcIso); // ‘Z’ 포함 ISO-8601
            ZonedDateTime seoul = instant.atZone(ZoneId.of("Asia/Seoul"));
            return seoul.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (Exception e) {
            // 파싱 실패 시 원본 반환(혹은 null)
            return utcIso;
        }
    }

    public static void main(String[] args) throws Exception {
        // 1) 실행 환경
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // BOOTSTRAP 환경변수 우선, 없으면 기본값(오하이오)
        String bootstrapServers = System.getenv().getOrDefault(
                "BOOTSTRAP",
                "3.143.42.149:29092,3.143.42.149:39092,3.143.42.149:49092"
        );

        // 2) Kafka Source (오하이오)
        String sourceTopic = "confluent-audit-log-events";
        String sourceGroupId = "flink-audit-log-processor";

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(sourceTopic)
                .setGroupId(sourceGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 필요 시 SASL 설정 해제 주석 풀기
                 .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "OAUTHBEARER")
                .setProperty("sasl.login.callback.handler.class",
                        "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler")
                .setProperty("sasl.jaas.config",
                        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                                + "username=\"admin\" password=\"admin-secret\" "
                                + "metadataServerUrls=\"http://15.164.187.115:8090\";")
                .build();

        DataStream<String> jsonStream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaAuditLogSource_Ohio");

        // 3) 변환 파이프라인
        ObjectMapper mapper = new ObjectMapper();

        DataStream<ParsedLog> parsedStream = jsonStream
                // JSON → CloudEvent
                .map((MapFunction<String, CloudEvent>) value -> {
                    try {
                        return mapper.readValue(value, CloudEvent.class);
                    } catch (Exception e) {
                        System.err.println("JSON 파싱 실패: " + value);
                        return null;
                    }
                })
                .name("ParseJson")
                .filter(Objects::nonNull)
                // 미승인(granted == false)만 통과
                .filter(ev -> ev.data != null && ev.data.authorizationInfo != null && !ev.data.authorizationInfo.granted)
                .name("Filter_Unauthorized")
                // CloudEvent → ParsedLog 평탄화 + 시간대 변환
                .map((MapFunction<CloudEvent, ParsedLog>) ev -> {
                    ParsedLog out = new ParsedLog();
                    out.eventTimeUtc   = ev.time;
                    out.eventTimeSeoul = toSeoul(ev.time);

                    if (ev.data != null) {
                        out.methodName        = ev.data.methodName;
                        out.dataResourceName  = ev.data.resourceName;

                        if (ev.data.authenticationInfo != null) {
                            out.principal = ev.data.authenticationInfo.principal;
                        }
                        if (ev.data.authorizationInfo != null) {
                            out.granted         = ev.data.authorizationInfo.granted;
                            out.resourceType    = ev.data.authorizationInfo.resourceType;
                            out.authResourceName= ev.data.authorizationInfo.resourceName;
                            out.operation       = ev.data.authorizationInfo.operation;
                            out.patternType     = ev.data.authorizationInfo.patternType;
                        }
                        if (ev.data.requestMetadata != null) {
                            out.requestId = ev.data.requestMetadata.request_id;
                        }
                        if (ev.data.request != null) {
                            out.correlationId = ev.data.request.correlation_id;
                            out.clientId      = ev.data.request.client_id;
                        }
                        if (ev.data.clientAddress != null && !ev.data.clientAddress.isEmpty()) {
                            ClientAddress ca = ev.data.clientAddress.get(0);
                            if (ca != null) {
                                out.clientIp       = ca.ip;
                                out.clientPort     = ca.port;
                                out.clientInternal = ca.internal;
                            }
                        }
                    }
                    out.processingTime = System.currentTimeMillis();
                    return out;
                })
                .name("ExtractInfo");

        // 4) Kafka Sink (오하이오)
        String sinkTopic = "audit-log-flink";
        Properties producerProps = new Properties();
        // 필요 시 보안 설정
        producerProps.put("security.protocol", "SASL_PLAINTEXT");
        producerProps.put("sasl.mechanism", "OAUTHBEARER");
        producerProps.put("sasl.login.callback.handler.class",
                "io.confluent.kafka.clients.plugins.auth.token.TokenUserLoginCallbackHandler");
        producerProps.put("sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
                        + "username=\"admin\" password=\"admin-secret\" "
                        + "metadataServerUrls=\"http://15.164.187.115:8090\";");

        KafkaSink<ParsedLog> sink = KafkaSink.<ParsedLog>builder()
                .setBootstrapServers(bootstrapServers)
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<ParsedLog>builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(new ParsedLogSerializer())
                                .build()
                )
                .build();

        parsedStream.sinkTo(sink).name("unauthorized-access");

        // 5) 실행
        env.execute("Audit Log Transfer Job in Ohio");
    }
}