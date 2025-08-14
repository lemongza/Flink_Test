package com.example.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;

// JSON 역직렬화 시 클래스에 정의되지 않은 필드는 무시
@JsonIgnoreProperties(ignoreUnknown = true)

// Flink에서 Source 연산이 Kafka로부터 읽어올 JSON 데이터를 매핑하기 위한 데이터 모델(POJO)
// Flink에서 Kafka Source를 통해 읽으면 그대로 RawProfile 객체로 매핑
public class RawProfile {
    // 프로필 기본 정보
    private String name;
    private String phone;
    private String birthDate;
    private String address;
    private String email;
    private String homepage;
    private String role;

    // 메타 데이터
    private long ingestTime;
    private String recordId;
    private String sourceSystem;
    private String country;
    private long lastUpdate;

    // 기본 생성자 (Jackson 또는 Flink에서 역직렬화 시 필요)
    public RawProfile() {}

    // 전체 필드 초기화하는 생성자
    public RawProfile(
            String name,
            String phone,
            String birthDate,
            String address,
            String email,
            String homepage,
            String role,
            long ingestTime,
            String recordId,
            String sourceSystem,
            String country,
            long lastUpdate
    ) {
        this.name = name;
        this.phone = phone;
        this.birthDate = birthDate;
        this.address = address;
        this.email = email;
        this.homepage = homepage;
        this.role = role;
        this.ingestTime = ingestTime;
        this.recordId = recordId;
        this.sourceSystem = sourceSystem;
        this.country = country;
        this.lastUpdate = lastUpdate;
    }

    // --- setters (Flink POJO 규칙 & Jackson 역직렬화용) ---
    public void setName(String name) { this.name = name; }
    public void setPhone(String phone) { this.phone = phone; }
    public void setBirthDate(String birthDate) { this.birthDate = birthDate; }
    public void setAddress(String address) { this.address = address; }
    public void setEmail(String email) { this.email = email; }
    public void setHomepage(String homepage) { this.homepage = homepage; }
    public void setRole(String role) { this.role = role; }
    public void setIngestTime(long ingestTime) { this.ingestTime = ingestTime; }
    public void setRecordId(String recordId) { this.recordId = recordId; }
    public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }
    public void setCountry(String country) { this.country = country; }
    public void setLastUpdate(long lastUpdate) { this.lastUpdate = lastUpdate; }

    // Getter 메서드
    public String getName() {
        return name;
    }

    public String getPhone() {
        return phone;
    }

    public String getBirthDate() {
        return birthDate;
    }

    public String getAddress() {
        return address;
    }

    public String getEmail() {
        return email;
    }

    public String getHomepage() {
        return homepage;
    }

    public String getRole() {
        return role;
    }

    public long getIngestTime() {
        return ingestTime;
    }

    public String getRecordId() {
        return recordId;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public String getCountry() {
        return country;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    // equals() 메서드
    // name, phone, birthDate 세 필드가 동일하면 같은 객체로 간주
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RawProfile that = (RawProfile) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(phone, that.phone) &&
                Objects.equals(birthDate, that.birthDate);
    }

    // hashCode() 메서드
    // equals와 동일한 필드를 기반으로 해시값 생성
    @Override
    public int hashCode() { return Objects.hash(name, phone, birthDate); }


    // toString() 메서드
    // 객체의 모든 필드를 사람이 읽기 좋은 문자열로 변환
    @Override
    public String toString() {
        return "RawProfile {" +
                "name='" + name + '\'' +
                ", phone='" + phone + '\'' +
                ", birthDate='" + birthDate + '\'' +
                ", address='" + address + '\'' +
                ", email='" + email + '\'' +
                ", homepage='" + homepage + '\'' +
                ", role='" + role + '\'' +
                ", ingestTime=" + ingestTime +
                ", recordId='" + recordId + '\'' +
                ", sourceSystem='" + sourceSystem + '\'' +
                ", country='" + country + '\'' +
                ", lastUpdate=" + lastUpdate +
                '}';
    }
}
