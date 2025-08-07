package io.zeebe.monitor.rest;

import static org.springframework.http.HttpStatus.NOT_FOUND;

import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.CatchEvent;
import io.camunda.zeebe.model.bpmn.instance.ErrorEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.SequenceFlow;
import io.camunda.zeebe.model.bpmn.instance.ServiceTask;
import io.camunda.zeebe.model.bpmn.instance.TimerEventDefinition;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeTaskDefinition;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.monitor.entity.ElementInstanceStatistics;
import io.zeebe.monitor.entity.MessageSubscriptionEntity;
import io.zeebe.monitor.entity.ProcessEntity;
import io.zeebe.monitor.entity.ProcessInstanceEntity;
import io.zeebe.monitor.entity.TimerEntity;
import io.zeebe.monitor.repository.MessageSubscriptionRepository;
import io.zeebe.monitor.repository.ProcessInstanceRepository;
import io.zeebe.monitor.repository.ProcessRepository;
import io.zeebe.monitor.repository.TimerRepository;
import io.zeebe.monitor.rest.dto.BpmnElementInfo;
import io.zeebe.monitor.rest.dto.ElementInstanceState;
import io.zeebe.monitor.rest.dto.MessageSubscriptionDto;
import io.zeebe.monitor.rest.dto.ProcessDto;
import io.zeebe.monitor.rest.dto.ProcessInstanceListDto;
import io.zeebe.monitor.rest.dto.TimerDto;
import jakarta.transaction.Transactional;
import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.camunda.bpm.model.xml.instance.ModelElementInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.server.ResponseStatusException;

@Controller
public class ProcessesViewController extends AbstractViewController {

  static final List<String> PROCESS_INSTANCE_ENTERED_INTENTS = List.of("ELEMENT_ACTIVATED");
  static final List<String> PROCESS_INSTANCE_COMPLETED_INTENTS =
      List.of("ELEMENT_COMPLETED", "ELEMENT_TERMINATED");
  static final List<String> EXCLUDE_ELEMENT_TYPES =
      List.of(BpmnElementType.MULTI_INSTANCE_BODY.name());

  @Autowired private ProcessRepository processRepository;
  @Autowired private ProcessInstanceRepository processInstanceRepository;
  @Autowired private MessageSubscriptionRepository messageSubscriptionRepository;
  @Autowired private TimerRepository timerRepository;

  @GetMapping("/")
  public String index(final Map<String, Object> model, final Pageable pageable) {
    return processList(model, pageable);
  }

  @GetMapping("/views/processes")
  public String processList(final Map<String, Object> model, final Pageable pageable) {

    final long count = processRepository.count();

    // Fix field names in pageable for database compatibility
    final Pageable correctedPageable = correctFieldNames(pageable);

    final List<ProcessDto> processes = new ArrayList<>();
    try {
      // Use custom query to avoid large object fields that might be corrupted
      final var processPage = processRepository.findAllWithoutLargeObjects(correctedPageable);
      for (final Object[] row : processPage.getContent()) {
        final ProcessEntity processEntity = createProcessEntityFromRow(row);
        final ProcessDto dto = ProcessDto.from(processEntity, 0, 0);
        processes.add(dto);
      }
    } catch (Exception e) {
      // Fallback to empty list if there's still an issue
      System.err.println("Error fetching processes: " + e.getMessage());
    }

    model.put("processes", processes);
    model.put("count", count);

    addPaginationToModel(model, pageable, count);
    addDefaultAttributesToModel(model);

    return "process-list-view";
  }

  /**
   * Correct field names in Pageable to match database column names
   */
  private Pageable correctFieldNames(final Pageable pageable) {
    if (pageable.getSort().isUnsorted()) {
      // If no sorting specified, use default: timestamp desc, key desc
      return PageRequest.of(
          pageable.getPageNumber(), 
          pageable.getPageSize(), 
          Sort.by(Sort.Direction.DESC, "TIMESTAMP_").and(Sort.by(Sort.Direction.DESC, "KEY_"))
      );
    }

    // Map field names to database column names
    final List<Sort.Order> correctedOrders = new ArrayList<>();
    for (Sort.Order order : pageable.getSort()) {
      String property = order.getProperty();
      // Map common field names to database column names
      switch (property.toLowerCase()) {
        case "timestamp":
          property = "TIMESTAMP_";
          break;
        case "key":
          property = "KEY_";
          break;
        case "bpmnprocessid":
        case "bpmn_process_id":
          property = "BPMN_PROCESS_ID_";
          break;
        case "version":
          property = "VERSION_";
          break;
        default:
          // Keep original if no mapping found
          break;
      }
      correctedOrders.add(new Sort.Order(order.getDirection(), property));
    }

    return PageRequest.of(
        pageable.getPageNumber(),
        pageable.getPageSize(),
        Sort.by(correctedOrders)
    );
  }

  /**
   * Create ProcessEntity from database row without large object fields
   * Row format: [KEY_, BPMN_PROCESS_ID_, VERSION_, TIMESTAMP_]
   */
  private ProcessEntity createProcessEntityFromRow(final Object[] row) {
    final ProcessEntity entity = new ProcessEntity();
    entity.setKey(((Number) row[0]).longValue());
    entity.setBpmnProcessId((String) row[1]);
    entity.setVersion(((Number) row[2]).intValue());
    entity.setTimestamp(((Number) row[3]).longValue());
    return entity;
  }

  /**
   * Create ProcessDto safely without accessing large object fields that might be corrupted
   */
  private ProcessDto createSafeProcessDto(final ProcessEntity entity) {
    final ProcessDto dto = new ProcessDto();
    
    dto.setProcessDefinitionKey(entity.getKey());
    dto.setBpmnProcessId(entity.getBpmnProcessId());
    dto.setVersion(entity.getVersion());
    dto.setDeploymentTime(java.time.Instant.ofEpochMilli(entity.getTimestamp()).toString());
    
    // Try to get resource field, but handle large object issues gracefully
    try {
      dto.setResource(entity.getResource());
    } catch (Exception e) {
      System.err.println("Could not load resource for process " + entity.getKey() + ": " + e.getMessage());
      dto.setResource(null); // or set to empty string or placeholder
    }
    
    // Set counts to 0 since we're not calculating them in the error case
    dto.setCountRunning(0);
    dto.setCountEnded(0);
    
    return dto;
  }

  @GetMapping("/views/processes/{key}")
  @Transactional
  public String processDetail(
      @PathVariable("key") final long key, final Map<String, Object> model, final Pageable pageable) {

    final ProcessEntity process =
        processRepository
            .findByKey(key)
            .orElseThrow(
                () -> new ResponseStatusException(NOT_FOUND, "No process found with key: " + key))
                ;

    try {
      model.put("process", toDto(process));
    } catch (Exception e) {
      System.err.println("Error creating process DTO for key " + key + ": " + e.getMessage());
      model.put("process", createSafeProcessDto(process));
    }
    
    try {
      model.put("resource", getProcessResource(process));
    } catch (Exception e) {
      System.err.println("Error getting process resource for key " + key + ": " + e.getMessage());
      model.put("resource", ""); // Empty resource if large object is corrupted
    }

    final List<ElementInstanceState> elementInstanceStates = getElementInstanceStates(key);
    model.put("instance.elementInstances", elementInstanceStates);

    final long count = 10000L;

    final List<ProcessInstanceListDto> instances = new ArrayList<>();
    for (final ProcessInstanceEntity instanceEntity :
        processInstanceRepository.findByProcessDefinitionKey(key, pageable)) {
      instances.add(toDto(instanceEntity));
    }

    model.put("instances", instances);
    model.put("count", count);

    final List<TimerDto> timers =
        timerRepository.findByProcessDefinitionKeyAndProcessInstanceKeyIsNull(key).stream()
            .map(ProcessesViewController::toDto)
            .collect(Collectors.toList());
    model.put("timers", timers);

    final List<MessageSubscriptionDto> messageSubscriptions =
        messageSubscriptionRepository
            .findByProcessDefinitionKeyAndProcessInstanceKeyIsNull(key)
            .stream()
            .map(ProcessesViewController::toDto)
            .collect(Collectors.toList());
    model.put("messageSubscriptions", messageSubscriptions);

    try {
      if (process.getResourcetext() != null) {
        final var resourceAsStream = new ByteArrayInputStream(process.getResourcetext().getBytes());
        final var bpmn = Bpmn.readModelFromStream(resourceAsStream);
        model.put("instance.bpmnElementInfos", getBpmnElementInfos(bpmn));
      } else {
        try {
          final var resource = process.getResource();
          final var resourceAsStream = new ByteArrayInputStream(resource.getBytes());
          final var bpmn = Bpmn.readModelFromStream(resourceAsStream);
          model.put("instance.bpmnElementInfos", getBpmnElementInfos(bpmn));
          process.setResourcetext(resource);
          processRepository.save(process);
        } catch (Exception e) {
          System.err.println("Error accessing large object resource for process " + key + ": " + e.getMessage());
          model.put("instance.bpmnElementInfos", new ArrayList<>()); // Empty list if resource is corrupted
        }
      }
    } catch (Exception e) {
      System.err.println("Error processing BPMN for process " + key + ": " + e.getMessage());
      model.put("instance.bpmnElementInfos", new ArrayList<>());
    }

    addPaginationToModel(model, pageable, count);
    addDefaultAttributesToModel(model);

    return "process-detail-view";
  }

  ProcessDto toDto(final ProcessEntity processEntity) {
    final long processDefinitionKey = processEntity.getKey();

    final long running =
        processInstanceRepository.countByProcessDefinitionKeyAndEndIsNull(processDefinitionKey);
    final long ended =
        processInstanceRepository.countByProcessDefinitionKeyAndEndIsNotNull(processDefinitionKey);

    return ProcessDto.from(processEntity, running, ended);
  }

  static ProcessInstanceListDto toDto(final ProcessInstanceEntity instance) {

    final ProcessInstanceListDto dto = new ProcessInstanceListDto();
    dto.setProcessInstanceKey(instance.getKey());

    dto.setBpmnProcessId(instance.getBpmnProcessId());
    dto.setProcessDefinitionKey(instance.getProcessDefinitionKey());

    final boolean isEnded = instance.getEnd() != null && instance.getEnd() > 0;
    dto.setState(instance.getState());

    dto.setStartTime(Instant.ofEpochMilli(instance.getStart()).toString());

    if (isEnded) {
      dto.setEndTime(Instant.ofEpochMilli(instance.getEnd()).toString());
    }

    return dto;
  }

  static TimerDto toDto(final TimerEntity timer) {
    final TimerDto dto = new TimerDto();

    dto.setElementId(timer.getTargetElementId());
    dto.setState(timer.getState());
    dto.setDueDate(Instant.ofEpochMilli(timer.getDueDate()).toString());
    dto.setTimestamp(Instant.ofEpochMilli(timer.getTimestamp()).toString());
    dto.setElementInstanceKey(timer.getElementInstanceKey());

    final int repetitions = timer.getRepetitions();
    dto.setRepetitions(repetitions >= 0 ? String.valueOf(repetitions) : "∞");

    return dto;
  }

  static MessageSubscriptionDto toDto(final MessageSubscriptionEntity subscription) {
    final MessageSubscriptionDto dto = new MessageSubscriptionDto();

    dto.setKey(subscription.getId());
    dto.setMessageName(subscription.getMessageName());
    dto.setCorrelationKey(Optional.ofNullable(subscription.getCorrelationKey()).orElse(""));

    dto.setProcessInstanceKey(subscription.getProcessInstanceKey());
    dto.setElementInstanceKey(subscription.getElementInstanceKey());

    dto.setElementId(subscription.getTargetFlowNodeId());

    dto.setState(subscription.getState());
    dto.setTimestamp(Instant.ofEpochMilli(subscription.getTimestamp()).toString());

    dto.setOpen(subscription.getState().equalsIgnoreCase(MessageSubscriptionIntent.CREATED.name()));

    return dto;
  }

  static String getProcessResource(final ProcessEntity process) {
    try {
      final var resource = process.getResourcetext() != null ? process.getResourcetext() : process.getResource();
      if (resource == null) {
        return "";
      }
      // replace all backticks because they are used to enclose the content of the BPMN in the HTML
      return resource.replaceAll("`", "\"");
    } catch (Exception e) {
      System.err.println("Error accessing resource for process " + process.getKey() + ": " + e.getMessage());
      return ""; // Return empty string if large object is corrupted
    }
  }

  private List<ElementInstanceState> getElementInstanceStates(final long key) {

    final List<ElementInstanceStatistics> elementEnteredStatistics =
        processRepository.getElementInstanceStatisticsByKeyAndIntentIn(
            key, PROCESS_INSTANCE_ENTERED_INTENTS, EXCLUDE_ELEMENT_TYPES);

    final Map<String, Long> elementCompletedCount =
        processRepository
            .getElementInstanceStatisticsByKeyAndIntentIn(
                key, PROCESS_INSTANCE_COMPLETED_INTENTS, EXCLUDE_ELEMENT_TYPES)
            .stream()
            .collect(
                Collectors.toMap(
                    ElementInstanceStatistics::getElementId, ElementInstanceStatistics::getCount));

    return elementEnteredStatistics.stream()
            .map(
                s -> {
                  final ElementInstanceState state = new ElementInstanceState();

                  final String elementId = s.getElementId();
                  state.setElementId(elementId);

                  final long completedInstances = elementCompletedCount.getOrDefault(elementId, 0L);
                  final long enteredInstances = s.getCount();

                  state.setActiveInstances(enteredInstances - completedInstances);
                  state.setEndedInstances(completedInstances);

                  return state;
                })
            .collect(Collectors.toList());
  }

  static List<BpmnElementInfo> getBpmnElementInfos(final BpmnModelInstance bpmn) {
    final List<BpmnElementInfo> infos = new ArrayList<>();

    bpmn.getModelElementsByType(ServiceTask.class)
        .forEach(
            t -> {
              final var info = new BpmnElementInfo();
              info.setElementId(t.getId());
              final var jobType = t.getSingleExtensionElement(ZeebeTaskDefinition.class).getType();
              info.setInfo("job-type: " + jobType);

              infos.add(info);
            });

    bpmn.getModelElementsByType(SequenceFlow.class)
        .forEach(
            s -> {
              final var conditionExpression = s.getConditionExpression();

              if (conditionExpression != null && !conditionExpression.getTextContent().isEmpty()) {

                final var info = new BpmnElementInfo();
                info.setElementId(s.getId());
                final var condition = conditionExpression.getTextContent();
                info.setInfo("condition: " + condition);

                infos.add(info);
              }
            });

    bpmn.getModelElementsByType(CatchEvent.class)
        .forEach(
            catchEvent -> {
              final var info = new BpmnElementInfo();
              info.setElementId(catchEvent.getId());

              catchEvent
                  .getEventDefinitions()
                  .forEach(
                      eventDefinition -> {
                        if (eventDefinition instanceof ErrorEventDefinition errorEventDefinition) {
                          if (errorEventDefinition.getError() != null) {
                            info.setInfo("errorCode: " + errorEventDefinition.getError().getErrorCode());
                          } else {
                            info.setInfo("errorCode: <null>");
                          }
                          infos.add(info);
                        }

                        if (eventDefinition instanceof TimerEventDefinition timerEventDefinition) {

                          Optional.<ModelElementInstance>ofNullable(
                                  timerEventDefinition.getTimeCycle())
                              .or(() -> Optional.ofNullable(timerEventDefinition.getTimeDate()))
                              .or(() -> Optional.ofNullable(timerEventDefinition.getTimeDuration()))
                              .map(ModelElementInstance::getTextContent)
                              .ifPresent(
                                  timer -> {
                                    info.setInfo("timer: " + timer);
                                    infos.add(info);
                                  });
                        }
                      });
            });

    return infos;
  }
}
