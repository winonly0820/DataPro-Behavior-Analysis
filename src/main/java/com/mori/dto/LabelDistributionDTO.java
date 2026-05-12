package com.mori.dto;

public class LabelDistributionDTO {

    private String parentLabel;
    private String label;
    private Long userCount;

    public LabelDistributionDTO() {
    }

    public LabelDistributionDTO(String parentLabel, String label, Long userCount) {
        this.parentLabel = parentLabel;
        this.label = label;
        this.userCount = userCount;
    }

    public String getParentLabel() {
        return parentLabel;
    }

    public void setParentLabel(String parentLabel) {
        this.parentLabel = parentLabel;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Long getUserCount() {
        return userCount;
    }

    public void setUserCount(Long userCount) {
        this.userCount = userCount;
    }
}