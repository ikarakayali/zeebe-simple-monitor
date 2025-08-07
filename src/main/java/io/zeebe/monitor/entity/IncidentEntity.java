/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.monitor.entity;

import jakarta.persistence.*;
import org.hibernate.Length;

@Entity(name = "INCIDENT")
@Table(indexes = {
    // performance reason, because we use it in the
    // {@link io.zeebe.monitor.repository.IncidentRepository#findByProcessInstanceKey(long)}
    @Index(name = "incident_processInstanceKeyIndex", columnList = "PROCESS_INSTANCE_KEY_"),
})
public class IncidentEntity {

  @Id
  @Column(name = "KEY_")
  private long key;

  @Column(name = "BPMN_PROCESS_ID_")
  private String bpmnProcessId;

  @Column(name = "PROCESS_DEFINITION_KEY_")
  private long processDefinitionKey;

  @Column(name = "PROCESS_INSTANCE_KEY_")
  private long processInstanceKey;

  @Column(name = "ELEMENT_INSTANCE_KEY_")
  private long elementInstanceKey;

  @Column(name = "JOB_KEY_")
  private long jobKey;

  @Column(name = "ERROR_TYPE_")
  private String errorType;

  @Column(name = "ERROR_MSG_",length= Length.LONG16)
  @Lob
  private String errorMessage;

  @Column(name = "ERROR_MSG_TEXT_", length = Length.LONG32)
  private String errorMessageText;

  @Column(name = "CREATED_")
  private long created;

  @Column(name = "RESOLVED_")
  private Long resolved;

  // Default constructor for JPA
  public IncidentEntity() {}

  // Constructor for custom query without LOB field
  public IncidentEntity(long key, String bpmnProcessId, long processDefinitionKey, 
                       long processInstanceKey, long elementInstanceKey, long jobKey, 
                       String errorType, String errorMessageText, long created, Long resolved) {
    this.key = key;
    this.bpmnProcessId = bpmnProcessId;
    this.processDefinitionKey = processDefinitionKey;
    this.processInstanceKey = processInstanceKey;
    this.elementInstanceKey = elementInstanceKey;
    this.jobKey = jobKey;
    this.errorType = errorType;
    this.errorMessageText = errorMessageText;
    this.created = created;
    this.resolved = resolved;
    // errorMessage (LOB) is intentionally not set - will be loaded lazily if needed
  }

  public String getErrorType() {
    return errorType;
  }

  public IncidentEntity setErrorType(final String errorType) {
    this.errorType = errorType;
    return this;
  }

  public String getErrorMessage() {
    return getSafeErrorMessage();
  }

  /**
   * Safely gets the error message using only the TEXT field to avoid LOB transaction issues
   */
  public String getSafeErrorMessage() {
    // First try the TEXT field (safer and already loaded)
    if (this.errorMessageText != null && !this.errorMessageText.trim().isEmpty()) {
      return this.errorMessageText;
    }
    
    // Then try LOB field if it was loaded
    if (this.errorMessage != null) {
      try {
        return this.errorMessage;
      } catch (Exception e) {
        // LOB access failed
      }
    }
    
    // If both are empty/failed, return unavailable message
    return "Error message unavailable";
  }

  /**
   * Direct access to the LOB field without safety checks
   */
  public String getErrorMessageLob() {
    return errorMessage;
  }

  /**
   * Check if we might need to load the LOB field
   * Returns true if TEXT field is empty but we haven't tried to load LOB yet
   */
  public boolean shouldLoadLobField() {
    return (this.errorMessageText == null || this.errorMessageText.trim().isEmpty()) 
           && this.errorMessage == null;
  }

  public IncidentEntity setErrorMessage(final String errorMessage) {
    this.errorMessage = errorMessage;
    return this;
  }

  public String getErrorMessageText() {
    return errorMessageText;
  }

  public IncidentEntity setErrorMessageText(final String errorMessageText) {
    this.errorMessageText = errorMessageText;
    return this;
  }

  public long getKey() {
    return key;
  }

  public void setKey(final long incidentKey) {
    this.key = incidentKey;
  }

  public String getBpmnProcessId() {
    return bpmnProcessId;
  }

  public IncidentEntity setBpmnProcessId(final String bpmnProcessId) {
    this.bpmnProcessId = bpmnProcessId;
    return this;
  }

  public long getProcessDefinitionKey() {
    return processDefinitionKey;
  }

  public IncidentEntity setProcessDefinitionKey(final long processDefinitionKey) {
    this.processDefinitionKey = processDefinitionKey;
    return this;
  }

  public long getProcessInstanceKey() {
    return processInstanceKey;
  }

  public void setProcessInstanceKey(final long processInstanceKey) {
    this.processInstanceKey = processInstanceKey;
  }

  public long getElementInstanceKey() {
    return elementInstanceKey;
  }

  public void setElementInstanceKey(final long elementInstanceKey) {
    this.elementInstanceKey = elementInstanceKey;
  }

  public long getJobKey() {
    return jobKey;
  }

  public void setJobKey(final long jobKey) {
    this.jobKey = jobKey;
  }

  public long getCreated() {
    return created;
  }

  public void setCreated(final long created) {
    this.created = created;
  }

  public Long getResolved() {
    return resolved;
  }

  public void setResolved(final Long resolved) {
    this.resolved = resolved;
  }
}
