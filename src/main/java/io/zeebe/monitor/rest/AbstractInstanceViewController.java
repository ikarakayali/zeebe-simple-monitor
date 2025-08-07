package io.zeebe.monitor.rest;

import static io.zeebe.monitor.rest.ProcessesViewController.EXCLUDE_ELEMENT_TYPES;
import static io.zeebe.monitor.rest.ProcessesViewController.PROCESS_INSTANCE_COMPLETED_INTENTS;
import static io.zeebe.monitor.rest.ProcessesViewController.PROCESS_INSTANCE_ENTERED_INTENTS;
import static org.springframework.http.HttpStatus.NOT_FOUND;

import com.samskivert.mustache.Mustache;
import com.samskivert.mustache.Template;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.monitor.entity.ElementInstanceEntity;
import io.zeebe.monitor.entity.IncidentEntity;
import io.zeebe.monitor.entity.ProcessInstanceEntity;
import io.zeebe.monitor.repository.ElementInstanceRepository;
import io.zeebe.monitor.repository.IncidentRepository;
import io.zeebe.monitor.repository.ProcessInstanceRepository;
import io.zeebe.monitor.repository.ProcessRepository;
import io.zeebe.monitor.rest.dto.ActiveScope;
import io.zeebe.monitor.rest.dto.ElementInstanceState;
import io.zeebe.monitor.rest.dto.ProcessInstanceDto;
import java.io.IOException;
import java.io.Writer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class AbstractInstanceViewController extends AbstractViewController {

  protected static final int DETAIL_LIST_SIZE = 100;
  protected static final String WARNING_NO_XML_RESOURCE_FOUND = "WARNING-NO-XML-RESOURCE-FOUND";

  @Autowired protected ProcessRepository processRepository;
  @Autowired protected ProcessInstanceRepository processInstanceRepository;
  @Autowired protected ElementInstanceRepository elementInstanceRepository;
  @Autowired protected IncidentRepository incidentRepository;
  @Autowired private JdbcTemplate jdbcTemplate;

  protected void initializeProcessInstanceDto(
      long key, Map<String, Object> model, Pageable pageable) {
    final ProcessInstanceEntity instance = loadProcessInstanceEntity(key);

    fillBpmnDataIntoModel(model, instance);

    final ProcessInstanceDto dto = fillBpmnDetailsIntoDto(instance);

    final Map<Long, String> elementIdsForKeys = new HashMap<>();
    elementIdsForKeys.put(instance.getKey(), instance.getBpmnProcessId());

    final List<ElementInstanceEntity> events = loadElementInstanceEntities(instance);
    events.forEach(e -> elementIdsForKeys.put(e.getKey(), e.getElementId()));

    final List<IncidentEntity> incidents = loadIncidents(instance);

    fillActivityInformationForDiagramAnnotationIntoDto(events, dto, incidents, elementIdsForKeys);

    // <<abstract>> per each child view
    fillViewDetailsIntoDto(instance, events, incidents, elementIdsForKeys, model, pageable, dto);

    fillActiveScopesIntoDto(instance, events, elementIdsForKeys, dto);

    model.put("instance", dto);
    addDefaultAttributesToModel(model);
  }

  protected ProcessInstanceEntity loadProcessInstanceEntity(long key) {
    return processInstanceRepository
        .findByKey(key)
        .orElseThrow(
            () ->
                new ResponseStatusException(
                    NOT_FOUND, "No process instance found with key: " + key));
  }

  private void fillBpmnDataIntoModel(Map<String, Object> model, ProcessInstanceEntity instance) {
    model.put("resource", WARNING_NO_XML_RESOURCE_FOUND);
    processRepository
        .findByKey(instance.getProcessDefinitionKey())
        .ifPresent(
            process -> model.put("resource", ProcessesViewController.getProcessResource(process)));
  }

  private List<IncidentEntity> loadIncidents(ProcessInstanceEntity instance) {
    try {
      // Step 1: Get incidents without LOB fields (safe and fast)
      List<IncidentEntity> incidents = StreamSupport.stream(
              incidentRepository.findByProcessInstanceKeyWithoutLob(instance.getKey()).spliterator(), false)
          .collect(Collectors.toList());
      
      // Step 2: For incidents with empty TEXT field, try to get LOB using simple approach
      incidents.parallelStream().forEach(incident -> {
        if (incident.shouldLoadLobField()) {
          loadLobSimply(incident);
        }
      });
      
      return incidents;
    } catch (Exception e) {
      // If even the safe query fails, return empty list
      return new ArrayList<>();
    }
  }

  /**
   * Simple LOB loading approach - handles PostgreSQL LOB storage
   */
  private void loadLobSimply(IncidentEntity incident) {
    try {
      // First try: Check if ERROR_MSG_ is stored as TEXT directly
      String lobValue = jdbcTemplate.queryForObject(
          "SELECT ERROR_MSG_ FROM INCIDENT WHERE KEY_ = ?", 
          String.class, 
          incident.getKey()
      );
      
      if (lobValue != null && !lobValue.trim().isEmpty()) {
          // Get the actual LOB content from pg_largeobject
          String lobContent = jdbcTemplate.queryForObject(
              "SELECT data FROM pg_largeobject WHERE loid = ? ORDER BY pageno", 
              String.class, 
              Long.parseLong(lobValue)
          );
          
          if (lobContent != null && !lobContent.trim().isEmpty()) {
            // PostgreSQL returns hex-encoded data, decode it
            String decodedContent = decodeHexString(lobContent);
            if (decodedContent != null && !decodedContent.trim().isEmpty()) {
              incident.setErrorMessage(decodedContent);
            }
          }
        }
        
      } catch (Exception lobException) {
        // Both approaches failed, continue silently with TEXT field
      }
    
  }

  /**
   * Decode hex-encoded string from PostgreSQL LOB data
   */
  private String decodeHexString(String hexString) {
    try {
      // Remove \x prefix if present
      String hex = hexString.startsWith("\\x") ? hexString.substring(2) : hexString;
      
      // Convert hex to bytes
      byte[] bytes = new byte[hex.length() / 2];
      for (int i = 0; i < hex.length(); i += 2) {
        bytes[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
      }
      
      // Convert bytes to UTF-8 string
      return new String(bytes, "UTF-8");
    } catch (Exception e) {
      return null;
    }
  }

  private ProcessInstanceDto fillBpmnDetailsIntoDto(ProcessInstanceEntity instance) {
    final ProcessInstanceDto dto = new ProcessInstanceDto();
    dto.setProcessInstanceKey(instance.getKey());
    dto.setPartitionId(instance.getPartitionId());
    dto.setProcessDefinitionKey(instance.getProcessDefinitionKey());
    dto.setBpmnProcessId(instance.getBpmnProcessId());
    dto.setVersion(instance.getVersion());
    final boolean isEnded = instance.getEnd() != null && instance.getEnd() > 0;
    dto.setState(instance.getState());
    dto.setRunning(!isEnded);
    dto.setStartTime(Instant.ofEpochMilli(instance.getStart()).toString());
    if (isEnded) {
      dto.setEndTime(Instant.ofEpochMilli(instance.getEnd()).toString());
    }
    if (instance.getParentElementInstanceKey() > 0) {
      dto.setParentProcessInstanceKey(instance.getParentProcessInstanceKey());
      processInstanceRepository
          .findByKey(instance.getParentProcessInstanceKey())
          .ifPresent(
              parent -> {
                dto.setParentBpmnProcessId(parent.getBpmnProcessId());
              });
    }
    return dto;
  }

  private void fillActivityInformationForDiagramAnnotationIntoDto(
      List<ElementInstanceEntity> events,
      ProcessInstanceDto dto,
      List<IncidentEntity> incidents,
      Map<Long, String> elementIdsForKeys) {
    final List<String> completedActivities =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .filter(e -> !e.getBpmnElementType().equals(BpmnElementType.PROCESS.name()))
            .map(ElementInstanceEntity::getElementId)
            .collect(Collectors.toList());

    final List<String> activeActivities =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
            .filter(e -> !e.getBpmnElementType().equals(BpmnElementType.PROCESS.name()))
            .map(ElementInstanceEntity::getElementId)
            .filter(id -> !completedActivities.contains(id))
            .collect(Collectors.toList());
    dto.setActiveActivities(activeActivities);

    final List<String> takenSequenceFlows =
        events.stream()
            .filter(e -> e.getIntent().equals("SEQUENCE_FLOW_TAKEN"))
            .map(ElementInstanceEntity::getElementId)
            .collect(Collectors.toList());
    dto.setTakenSequenceFlows(takenSequenceFlows);

    final Map<String, Long> completedElementsById =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .filter(e -> !EXCLUDE_ELEMENT_TYPES.contains(e.getBpmnElementType()))
            .collect(
                Collectors.groupingBy(ElementInstanceEntity::getElementId, Collectors.counting()));

    final Map<String, Long> enteredElementsById =
        events.stream()
            .filter(e -> PROCESS_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
            .filter(e -> !EXCLUDE_ELEMENT_TYPES.contains(e.getBpmnElementType()))
            .collect(
                Collectors.groupingBy(ElementInstanceEntity::getElementId, Collectors.counting()));

    final List<ElementInstanceState> elementStates =
        enteredElementsById.entrySet().stream()
            .map(
                e -> {
                  final String elementId = e.getKey();

                  final long enteredInstances = e.getValue();
                  final long completedInstances = completedElementsById.getOrDefault(elementId, 0L);

                  final ElementInstanceState state = new ElementInstanceState();
                  state.setElementId(elementId);
                  state.setActiveInstances(enteredInstances - completedInstances);
                  state.setEndedInstances(completedInstances);

                  return state;
                })
            .collect(Collectors.toList());

    dto.setElementInstances(elementStates);

    final List<String> activitiesWitIncidents =
        incidents.stream()
            .filter(i -> i.getResolved() == null || i.getResolved() <= 0)
            .map(i -> elementIdsForKeys.get(i.getElementInstanceKey()))
            .distinct()
            .collect(Collectors.toList());

    dto.setIncidentActivities(activitiesWitIncidents);

    activeActivities.removeAll(activitiesWitIncidents);
    dto.setActiveActivities(activeActivities);
  }

  protected abstract void fillViewDetailsIntoDto(
      final ProcessInstanceEntity instance,
      List<ElementInstanceEntity> events,
      List<IncidentEntity> incidents,
      Map<Long, String> elementIdsForKeys,
      Map<String, Object> model,
      Pageable pageable,
      ProcessInstanceDto dto);

  protected void fillActiveScopesIntoDto(
      ProcessInstanceEntity instance,
      List<ElementInstanceEntity> events,
      Map<Long, String> elementIdsForKeys,
      ProcessInstanceDto dto) {
    final List<ActiveScope> activeScopes = new ArrayList<>();
    if (dto.isRunning()) {
      activeScopes.add(new ActiveScope(instance.getKey(), instance.getBpmnProcessId()));

      final List<Long> completedElementInstances =
          events.stream()
              .filter(e -> PROCESS_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
              .map(ElementInstanceEntity::getKey)
              .collect(Collectors.toList());

      final List<ActiveScope> activeElementInstances =
          events.stream()
              .filter(e -> PROCESS_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
              .map(ElementInstanceEntity::getKey)
              .filter(id -> !completedElementInstances.contains(id))
              .map(scopeKey -> new ActiveScope(scopeKey, elementIdsForKeys.get(scopeKey)))
              .collect(Collectors.toList());

      activeScopes.addAll(activeElementInstances);
    }
    dto.setActiveScopes(activeScopes);
  }

  private List<ElementInstanceEntity> loadElementInstanceEntities(ProcessInstanceEntity instance) {
    return StreamSupport.stream(
            elementInstanceRepository.findByProcessInstanceKey(instance.getKey()).spliterator(),
            false)
        .collect(Collectors.toList());
  }

  /** helper class to allow conditional server side rendering in the mustache templates */
  static class EnableConditionalViewRenderer implements Mustache.Lambda {
    @Override
    public void execute(Template.Fragment frag, Writer out) throws IOException {
      out.append(frag.execute());
    }
  }
}
