/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.shared.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.camunda.zeebe.gateway.admin.backup.BackupApi;
import io.camunda.zeebe.gateway.admin.backup.BackupStatus;
import io.camunda.zeebe.gateway.admin.backup.PartitionBackupStatus;
import io.camunda.zeebe.gateway.admin.backup.State;
import io.camunda.zeebe.protocol.management.BackupStatusCode;
import io.camunda.zeebe.shared.management.BackupEndpoint.ErrorResponse;
import io.camunda.zeebe.shared.management.BackupEndpoint.TakeBackupResponse;
import io.camunda.zeebe.shared.management.openapi.models.BackupInfo;
import io.camunda.zeebe.shared.management.openapi.models.Error;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;

@Execution(ExecutionMode.CONCURRENT)
final class BackupEndpointTest {
  @Nested
  final class TakeTest {
    @Test
    void shouldReturnErrorOnCompletionException() {
      // given
      final var api = mock(BackupApi.class);
      final var endpoint = new BackupEndpoint(api);
      final var failure = new RuntimeException("failure");
      doReturn(CompletableFuture.failedFuture(failure)).when(api).takeBackup(anyLong());

      // when
      final WebEndpointResponse<?> response = endpoint.take(1);

      // then
      assertThat(response.getBody())
          .asInstanceOf(InstanceOfAssertFactories.type(ErrorResponse.class))
          .isEqualTo(new ErrorResponse(1, "failure"));
    }

    @Test
    void shouldReturnErrorOnException() {
      // given
      final var api = mock(BackupApi.class);
      final var endpoint = new BackupEndpoint(api);
      final var failure = new RuntimeException("failure");
      doThrow(failure).when(api).takeBackup(anyLong());

      // when
      final WebEndpointResponse<?> response = endpoint.take(1);

      // then
      assertThat(response.getBody())
          .asInstanceOf(InstanceOfAssertFactories.type(ErrorResponse.class))
          .isEqualTo(new ErrorResponse(1, "failure"));
    }

    @Test
    void shouldReturnNewBackupIdOnSuccess() {
      // given
      final var api = mock(BackupApi.class);
      final var endpoint = new BackupEndpoint(api);
      doReturn(CompletableFuture.completedFuture(3L)).when(api).takeBackup(anyLong());

      // when
      final WebEndpointResponse<?> response = endpoint.take(1);

      // then
      assertThat(response.getBody())
          .asInstanceOf(InstanceOfAssertFactories.type(TakeBackupResponse.class))
          .isEqualTo(new TakeBackupResponse(3));
    }
  }

  @Nested
  final class StatusTest {
    @Test
    void shouldReturnErrorOnCompletionException() {
      // given
      final var api = mock(BackupApi.class);
      final var endpoint = new BackupEndpoint(api);
      final var failure = new RuntimeException("failure");
      doReturn(CompletableFuture.failedFuture(failure)).when(api).getStatus(anyLong());

      // when
      final WebEndpointResponse<?> response = endpoint.status(1);

      // then
      assertThat(response.getBody())
          .asInstanceOf(InstanceOfAssertFactories.type(Error.class))
          .isEqualTo(new Error().message("failure"));
    }

    @Test
    void shouldReturnErrorOnException() {
      // given
      final var api = mock(BackupApi.class);
      final var endpoint = new BackupEndpoint(api);
      final var failure = new RuntimeException("failure");
      doThrow(failure).when(api).getStatus(anyLong());

      // when
      final WebEndpointResponse<?> response = endpoint.status(1);

      // then
      assertThat(response.getBody())
          .asInstanceOf(InstanceOfAssertFactories.type(Error.class))
          .isEqualTo(new Error().message("failure"));
    }

    @Test
    void shouldReturn404WhenBackupDoesNotExist() {
      // given
      final var api = mock(BackupApi.class);
      final var endpoint = new BackupEndpoint(api);
      final var backupStatus =
          new BackupStatus(1, State.DOES_NOT_EXIST, Optional.empty(), List.of());
      doReturn(CompletableFuture.completedFuture(backupStatus)).when(api).getStatus(anyLong());

      // when
      final WebEndpointResponse<?> response = endpoint.status(1);

      // then
      assertThat(response.getStatus()).isEqualTo(404);
      assertThat(response.getBody())
          .asInstanceOf(InstanceOfAssertFactories.type(Error.class))
          .isEqualTo(new Error().message("Backup with id 1 does not exist"));
    }

    @Test
    void shouldReturnCompletedBackupStatus() throws JsonProcessingException {
      // given
      final var api = mock(BackupApi.class);
      final var endpoint = new BackupEndpoint(api);
      final var status = createPartitionBackupStatus();
      doReturn(CompletableFuture.completedFuture(status)).when(api).getStatus(anyLong());

      final String expectedJson =
          """
          {
             "backupId" : 1,
             "state" : "COMPLETED",
             "details": [
               {
                 "partitionId": 1,
                 "state": "COMPLETED",
                 "createdAt": "2022-09-19T14:44:17.340409393Z",
                 "lastUpdatedAt": "2022-09-20T14:44:17.340409393Z",
                 "snapshotId" : "1-1-1-1",
                 "checkpointPosition": 1,
                 "brokerId": 0,
                 "brokerVersion": "8.0.6"
               },
               {
                 "partitionId": 2,
                 "state": "COMPLETED",
                 "createdAt": "2022-09-19T14:44:17.340409393Z",
                 "lastUpdatedAt": "2022-09-20T14:44:17.340409393Z",
                 "snapshotId" : "1-1-1-1",
                 "checkpointPosition": 1,
                 "brokerId": 0,
                 "brokerVersion": "8.0.6"
               }
             ]
           }
          """;
      final ObjectMapper mapper = new ObjectMapper();
      mapper.registerModule(new JavaTimeModule());
      final BackupInfo expectedResponse = mapper.readValue(expectedJson, BackupInfo.class);

      // when
      final WebEndpointResponse<?> response = endpoint.status(1);

      // then
      assertThat(response.getBody())
          .asInstanceOf(InstanceOfAssertFactories.type(BackupInfo.class))
          .isEqualTo(expectedResponse);
    }

    @Test
    void shouldReturnFailedBackupStatus() throws JsonProcessingException {
      // given
      final var api = mock(BackupApi.class);
      final var endpoint = new BackupEndpoint(api);
      final var status = createFailedBackupStatus();
      doReturn(CompletableFuture.completedFuture(status)).when(api).getStatus(anyLong());

      final String expectedJson =
          """
          {
             "backupId" : 1,
             "state" : "FAILED",
             "failureReason": "Failed backup",
             "details": [
               {
                 "partitionId": 1,
                 "state": "COMPLETED",
                 "createdAt": "2022-09-19T14:44:17.340409393Z",
                 "lastUpdatedAt": "2022-09-20T14:44:17.340409393Z",
                 "snapshotId" : "1-1-1-1",
                 "checkpointPosition": 1,
                 "brokerId": 0,
                 "brokerVersion": "8.0.6"
               },
               {
                 "partitionId": 2,
                 "state": "FAILED",
                 "failureReason": "Failed backup"
               }
             ]
           }
          """;
      final ObjectMapper mapper = new ObjectMapper();
      mapper.registerModule(new JavaTimeModule());
      final BackupInfo expectedResponse = mapper.readValue(expectedJson, BackupInfo.class);

      // when
      final WebEndpointResponse<?> response = endpoint.status(1);

      // then
      assertThat(response.getBody())
          .asInstanceOf(InstanceOfAssertFactories.type(BackupInfo.class))
          .isEqualTo(expectedResponse);
    }

    private BackupStatus createPartitionBackupStatus() {
      return new BackupStatus(
          1L,
          State.COMPLETED,
          Optional.empty(),
          List.of(createPartitionBackupStatus(1), createPartitionBackupStatus(2)));
    }

    private PartitionBackupStatus createPartitionBackupStatus(final int partitionId) {
      return new PartitionBackupStatus(
          partitionId,
          BackupStatusCode.COMPLETED,
          Optional.empty(),
          Optional.of("2022-09-19T14:44:17.340409393Z"),
          Optional.of("2022-09-20T14:44:17.340409393Z"),
          Optional.of("1-1-1-1"),
          OptionalLong.of(1),
          OptionalInt.of(0),
          Optional.of("8.0.6"));
    }

    private BackupStatus createFailedBackupStatus() {
      return new BackupStatus(
          1L,
          State.FAILED,
          Optional.of("Failed backup"),
          List.of(createPartitionBackupStatus(1), createFailedPartitionBackupStatus(2)));
    }

    @SuppressWarnings("SameParameterValue")
    private PartitionBackupStatus createFailedPartitionBackupStatus(final int partitionId) {
      return new PartitionBackupStatus(
          partitionId,
          BackupStatusCode.FAILED,
          Optional.of("Failed backup"),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          OptionalLong.empty(),
          OptionalInt.empty(),
          Optional.empty());
    }
  }
}