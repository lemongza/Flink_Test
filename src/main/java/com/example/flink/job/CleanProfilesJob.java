package com.example.flink.job;

import com.example.flink.model.CleanProfile;
import com.example.flink.model.RawProfile;
import com.example.flink.kafka.SinkProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.example.flink.config.TestConfig;
import org.apache.flink.api.common.typeinfo.Types;

import java.time.Instant;

// 카프카에서 원본 프로필 JSON 데이터를 읽어옴
// 다음 규칙으로 정제함
// 새 카프카 토픽에 정재하는 Job

public class CleanProfilesJob {
    //JSON 직렬화/역직렬화 위한 ObjectMapper
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 1️⃣ Flink 실행 환경 생성
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2️⃣ 설정 파일 로딩 (src/main/resources/config.properties)
        final TestConfig config = new TestConfig();
        final String brokers  = config.getBrokers(); // 브로커 주소 목록
        final String srcTopic = config.getSourceTopic(); // 원본 데이터 토픽
        final String dstTopic = config.getSinkTopic(); // 정제 데이터 토픽
        final String groupId  = config.getGroupId(); // 카프카 컨슈머 그룹 ID
        final String offsetReset = config.getOffsetReset(); // earliest or latest

        // earliest : 해당 Consumer Group이 아직 읽은 위치(오프셋) 기록이 없을 때, 토픽의 가장 처음(최소 오프셋)부터 읽기 시작
        // latest : 해당 Consumer Group이 아직 읽은 위치 기록이 없을 때, 현재 시점 이후에 들어오는 새 메시지부터 읽기 시작

        // 3️⃣ 오프셋 초기화 방식 결정 (기본 earliest)
        final OffsetsInitializer offsets =
                "latest".equalsIgnoreCase(offsetReset) ? OffsetsInitializer.latest() : OffsetsInitializer.earliest();

        // 4️⃣ Kafka에서 RawProfile JSON 데이터를 읽어 DataStream<RawProfile> 생성
        // SourceProfile 유틸을 통해 JSON -> RawProfile POJO 자동 매핑
        // 💬 POJO (Plain Old Java Object) : 특별한 제약이나 상속 없이, 순수하게 데이터를 담기 위한 일반 자바 객체
        DataStream<RawProfile> parsed = com.example.flink.kafka.SourceProfile.streamRawProfile(
                env, brokers, srcTopic, groupId, offsets);

        //earliest부터 RawProfile로 읽기
        //DataStream<RawProfile> parsed = com.example.flink.kafka.SourceProfile.streamRawProfileEarliest(env, brokers, srcTopic, groupId);

        // 5️⃣ 데이터 정제 로직 적용 (MapFunction 사용)
        DataStream<CleanProfile> cleaned = parsed
                .map((MapFunction<RawProfile, CleanProfile>) rp -> {
                    long now = Instant.now().toEpochMilli(); // 정제 시각 기록
                    return new CleanProfile(
                            rp.getEmail(),
                            maskName(rp.getName()), // 이름 마스킹
                            formatPhone(rp.getPhone()), // 전화번호 포맷 변환
                            toYYMMDD(rp.getBirthDate()), // 생년월일 변환
                            trimAddress(rp.getAddress()), // 주소 축약
                            rp.getRole(),
                            rp.getRecordId(),
                            rp.getSourceSystem(),
                            rp.getCountry(),
                            now
                    );
                })
                .returns(CleanProfile.class); // 정제된 데이터 반환

        // 6️⃣ CleanProfile 객체를 JSON 문자열로 직렬화
        final ObjectMapper om = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        DataStream<String> outJson = cleaned
                .map((MapFunction<CleanProfile, String>) om::writeValueAsString)
                .returns(Types.STRING);

        // 7️⃣ Kafka Sink 생성 -> 정제 데이터 토픽으로 전송
        //outJson.sinkTo(SinkProfile.createKafkaSink(brokers, dstTopic));
        outJson
            .map(value -> Tuple2.of("myKey", value)) // Key 지정
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .sinkTo(SinkProfile.createKafkaSinkWithKey(brokers, dstTopic));

        // 8️⃣ Flink Job
        // 여기서 카프카 적재됨
        // 정제 로직이 적용된 데이터 처리 시작
        // env 단계에서 지금까지 설정한 소스, 변환, 싱크 연산들을 실행 계획 형태로 쌓여 있음
        // execute : Flink가 이 실행 계획을 JobGraph로 변환
        // 클러스터/로컬 환경에 제출 -> 병렬로 연산(task) 시작
        // 이후 Kafka Source에서 데이터를 읽고 → map 변환(정제 로직) → Kafka Sink에 쓰는 흐름이 실시간 스트리밍으로 계속 반복
        env.execute("Clean Profiles Job");
    }

    // ====== 유틸 함수들 ======

    // 1) 이름 마스킹: 첫 글자 + '*' + 마지막 글자 (2글자면 앞글자+*)
    private static String maskName(String name) {
        if (name == null || name.isBlank()) return "";
        name = name.trim();
        if (name.length() == 1) return name;
        if (name.length() == 2) return name.charAt(0) + "*";
        return name.charAt(0) + "*" + name.substring(name.length() - 1);
    }

    // 2) 전화번호 포맷: 아무 문자나 제거 → 국가코드(+82)면 0으로 치환 → 3-4-4 형태
    private static String formatPhone(String phone) {
        if (phone == null) return "";
        // 숫자와 '+'만 남김
        String d = phone.replaceAll("[^0-9+]", "");
        // +82 → 0 접두로 치환 (+8210XXXX → 010XXXX)
        if (d.startsWith("+82")) {
            d = "0" + d.substring(3);
        }
        // 나머지 비숫자 제거
        d = d.replaceAll("[^0-9]", "");
        // 11자리(010xxxxxxxx) 기준 분할, 길이가 다르면 가능한 한 안전하게 분할
        if (d.length() >= 11) {
            return d.substring(0, 3) + "-" + d.substring(3, 7) + "-" + d.substring(7, 11);
        } else if (d.length() == 10) { // 구형 10자리 예외 처리
            return d.substring(0, 3) + "-" + d.substring(3, 6) + "-" + d.substring(6);
        } else if (d.length() > 3 && d.length() <= 8) {
            // 임시 데이터 등: 3-나머지 분할
            return d.substring(0, 3) + "-" + d.substring(3);
        }
        return d; // 포맷 불가 시 원시 숫자열 반환
    }

    // 3) 생년월일 → YYMMDD (입력이 YYYY-MM-DD 또는 YYYYMMDD 가정)
    private static String toYYMMDD(String birthDate) {
        if (birthDate == null || birthDate.isBlank()) return "";
        String digits = birthDate.replaceAll("[^0-9]", "");
        if (digits.length() >= 8) {
            String yy = digits.substring(2, 4);
            String mm = digits.substring(4, 6);
            String dd = digits.substring(6, 8);
            return yy + mm + dd;
        }
        // 길이가 애매하면 그대로 반환
        return digits;
    }

    // 4) 주소: "…구"가 있으면 구까지, 없으면 "…시"까지만 남김
    private static String trimAddress(String address) {
        if (address == null || address.isBlank()) return "";
        address = address.trim();
        int idxGu = address.indexOf('구');
        if (idxGu >= 0) return address.substring(0, idxGu + 1);
        int idxSi = address.indexOf('시');
        if (idxSi >= 0) return address.substring(0, idxSi + 1);
        // 그래도 없으면 첫 공백 전까지
        int sp = address.indexOf(' ');
        return sp > 0 ? address.substring(0, sp) : address;
    }
}
