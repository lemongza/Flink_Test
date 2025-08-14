package com.example.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.util.Objects;

// Flink 정제 후 Sink로 내보낼 데이터 모델
// 원본 데이터(RawProfile)를 변환하여 저장하는 DTO(Data Transfer Object)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CleanProfile implements Serializable {
    // 필드 정의
    private String email;          // 식별자(키)
    private String nameMasked;     // 가운데 * 마스킹
    private String phoneFormatted; // 010-1234-5678
    private String birthYYMMDD;    // 예: 971010
    private String region;         // 주소에서 ~시|~구 까지만
    private String role;
    private String recordId;
    private String sourceSystem;
    private String country;
    private long processedTime;    // 정제 타임스탬프

    // 기본 생성자
    public CleanProfile() {}

    // 전체 필드 생성자
    public CleanProfile(String email, String nameMasked, String phoneFormatted, String birthYYMMDD,
                        String region, String role, String recordId, String sourceSystem,
                        String country, long processedTime) {
        this.email = email;
        this.nameMasked = nameMasked;
        this.phoneFormatted = phoneFormatted;
        this.birthYYMMDD = birthYYMMDD;
        this.region = region;
        this.role = role;
        this.recordId = recordId;
        this.sourceSystem = sourceSystem;
        this.country = country;
        this.processedTime = processedTime;
    }

    // Getter 메서드
    public String getEmail() { return email; }
    public String getNameMasked() { return nameMasked; }
    public String getPhoneFormatted() { return phoneFormatted; }
    public String getBirthYYMMDD() { return birthYYMMDD; }
    public String getRegion() { return region; }
    public String getRole() { return role; }
    public String getRecordId() { return recordId; }
    public String getSourceSystem() { return sourceSystem; }
    public String getCountry() { return country; }
    public long getProcessedTime() { return processedTime; }

    // Setter 메서드
    public void setEmail(String email) { this.email = email; }
    public void setNameMasked(String nameMasked) { this.nameMasked = nameMasked; }
    public void setPhoneFormatted(String phoneFormatted) { this.phoneFormatted = phoneFormatted; }
    public void setBirthYYMMDD(String birthYYMMDD) { this.birthYYMMDD = birthYYMMDD; }
    public void setRegion(String region) { this.region = region; }
    public void setRole(String role) { this.role = role; }
    public void setRecordId(String recordId) { this.recordId = recordId; }
    public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }
    public void setCountry(String country) { this.country = country; }
    public void setProcessedTime(long processedTime) { this.processedTime = processedTime; }

    // equals()
    // processedTime 포함 모든 주요 필드가 동일하면 같은 객체로 판단
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CleanProfile)) return false;
        CleanProfile that = (CleanProfile) o;
        return processedTime == that.processedTime &&
                Objects.equals(email, that.email) &&
                Objects.equals(nameMasked, that.nameMasked) &&
                Objects.equals(phoneFormatted, that.phoneFormatted) &&
                Objects.equals(birthYYMMDD, that.birthYYMMDD) &&
                Objects.equals(region, that.region) &&
                Objects.equals(role, that.role) &&
                Objects.equals(recordId, that.recordId) &&
                Objects.equals(sourceSystem, that.sourceSystem) &&
                Objects.equals(country, that.country);
    }

    // hashCode()
    // equals와 동일한 필드 기반으로 해시코드 생성
    @Override
    public int hashCode() {
        return Objects.hash(email, nameMasked, phoneFormatted, birthYYMMDD, region, role,
                recordId, sourceSystem, country, processedTime);
    }

    // toString()
    // 객체 내용을 사람이 읽기 좋은 문자열로 변환
    @Override
    public String toString() {
        return "CleanProfile{" +
                "email='" + email + '\'' +
                ", nameMasked='" + nameMasked + '\'' +
                ", phoneFormatted='" + phoneFormatted + '\'' +
                ", birthYYMMDD='" + birthYYMMDD + '\'' +
                ", region='" + region + '\'' +
                ", role='" + role + '\'' +
                ", recordId='" + recordId + '\'' +
                ", sourceSystem='" + sourceSystem + '\'' +
                ", country='" + country + '\'' +
                ", processedTime=" + processedTime +
                '}';
    }
}
