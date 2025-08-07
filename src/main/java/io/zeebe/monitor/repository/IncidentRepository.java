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
package io.zeebe.monitor.repository;

import io.zeebe.monitor.entity.IncidentEntity;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;

public interface IncidentRepository extends PagingAndSortingRepository<IncidentEntity, Long>, QuerydslPredicateExecutor<IncidentEntity>, CrudRepository<IncidentEntity, Long> {

  Iterable<IncidentEntity> findByProcessInstanceKey(long processInstanceKey);

  /**
   * Find incidents without loading LOB fields to avoid LOB access issues
   * This query excludes ERROR_MSG_ (LOB) field from the select to prevent lazy loading issues
   */
  @Query("SELECT new io.zeebe.monitor.entity.IncidentEntity(i.key, i.bpmnProcessId, i.processDefinitionKey, " +
         "i.processInstanceKey, i.elementInstanceKey, i.jobKey, i.errorType, i.errorMessageText, " +
         "i.created, i.resolved) " +
         "FROM INCIDENT i WHERE i.processInstanceKey = ?1")
  Iterable<IncidentEntity> findByProcessInstanceKeyWithoutLob(long processInstanceKey);

  /**
   * Get only the LOB field value for a specific incident - .NET style simple query
   */
  @Query(value = "SELECT ERROR_MSG_ FROM INCIDENT WHERE KEY_ = ?1", nativeQuery = true)
  String findErrorMessageLobByKey(long key);
}
