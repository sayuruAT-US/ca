package com.snet.transcriptprocessing.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;

import java.time.OffsetDateTime;

/**
 * Shared audit fields and lifecycle callbacks for all entity classes.
 * All entity classes extend this to avoid duplicating audit boilerplate.
 */
@MappedSuperclass
public abstract class AuditBaseEntity {

    @Column(name = "created_by", nullable = false, columnDefinition = "TEXT DEFAULT 'system'")
    @JsonProperty("created_by")
    private String createdBy = "system";

    @Column(name = "updated_by", nullable = false, columnDefinition = "TEXT DEFAULT 'system'")
    @JsonProperty("updated_by")
    private String updatedBy = "system";

    @Column(name = "created_at", nullable = false, updatable = false, columnDefinition = "TIMESTAMPTZ DEFAULT now()")
    @JsonProperty("created_at")
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false, columnDefinition = "TIMESTAMPTZ DEFAULT now()")
    @JsonProperty("updated_at")
    private OffsetDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        OffsetDateTime now = OffsetDateTime.now();
        if (this.createdAt == null) this.createdAt = now;
        if (this.updatedAt == null) this.updatedAt = now;
        if (this.createdBy == null || this.createdBy.isBlank()) this.createdBy = "system";
        if (this.updatedBy == null || this.updatedBy.isBlank()) this.updatedBy = "system";
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = OffsetDateTime.now();
        if (this.updatedBy == null || this.updatedBy.isBlank()) this.updatedBy = "system";
    }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    public String getUpdatedBy() { return updatedBy; }
    public void setUpdatedBy(String updatedBy) { this.updatedBy = updatedBy; }

    public OffsetDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(OffsetDateTime createdAt) { this.createdAt = createdAt; }

    public OffsetDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime updatedAt) { this.updatedAt = updatedAt; }
}
