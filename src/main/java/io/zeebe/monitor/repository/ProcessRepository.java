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

import io.zeebe.monitor.entity.ElementInstanceStatistics;
import io.zeebe.monitor.entity.ProcessEntity;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

public interface ProcessRepository
    extends PagingAndSortingRepository<ProcessEntity, Long>,
        QuerydslPredicateExecutor<ProcessEntity>,
        CrudRepository<ProcessEntity, Long> {

  Optional<ProcessEntity> findByKey(long key);

  @Query(
      nativeQuery = true,
      value =
          "SELECT ELEMENT_ID_ AS elementId, COUNT(*) AS count "
              + "FROM ELEMENT_INSTANCE "
              + "WHERE PROCESS_DEFINITION_KEY_ = :key and INTENT_ in (:intents) and BPMN_ELEMENT_TYPE_ not in (:excludeElementTypes)"
              + "GROUP BY ELEMENT_ID_")
  List<ElementInstanceStatistics> getElementInstanceStatisticsByKeyAndIntentIn(
      @Param("key") long key,
      @Param("intents") Collection<String> intents,
      @Param("excludeElementTypes") Collection<String> excludeElementTypes);

  @Query(
      nativeQuery = true,
      value =
          """
                  SELECT p.key_
                      FROM process p WHERE (bpmn_process_id_,version_) IN
                         (SELECT bpmn_process_id_, MAX(version_)  FROM process p  GROUP BY p.bpmn_process_id_)
                  """)
  List<Long> findLatestVersions();

  List<ProcessEntity> findByBpmnProcessIdContaining(String bpmnProcessId, Pageable pageable);
}
