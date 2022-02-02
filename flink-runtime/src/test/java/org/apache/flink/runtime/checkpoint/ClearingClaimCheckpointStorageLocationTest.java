/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.persistence.TestingStateHandleStore;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.Executors;

import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;

/**
 * Tests for deleting a {@link CheckpointStorageLocation} of a savepoint restored in {@link
 * RestoreMode#CLAIM}.
 */
@ExtendWith(TestLoggerExtension.class)
public class ClearingClaimCheckpointStorageLocationTest {
    @TempDir private Path checkpointPath;

    public static Stream<CompletedCheckpointStore> parameters() throws Exception {
        return Stream.of(
                createDefaultCompletedCheckpointStore(
                        TestingStateHandleStore.<CompletedCheckpoint>newBuilder()
                                .setRemoveFunction(ignored -> true)
                                .build()),
                new StandaloneCompletedCheckpointStore(
                        1,
                        (deleteExecutor, checkpoints) ->
                                SharedStateRegistry.DEFAULT_FACTORY.create(
                                        org.apache.flink.util.concurrent.Executors.directExecutor(),
                                        checkpoints),
                        Executors.directExecutor()));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testDisposingClaimedLocation(CompletedCheckpointStore checkpointStore) throws Exception {
        final Path path = checkpointPath;
        System.out.println(path.toString());
        final SharedStateRegistry stateRegistry = checkpointStore.getSharedStateRegistry();
        final FsCompletedCheckpointStorageLocation location =
                new FsCompletedCheckpointStorageLocation(
                        new LocalFileSystem(),
                        new org.apache.flink.core.fs.Path(path.toUri()),
                        null,
                        path.toString());

        final JobID job = new JobID();
        final Path sharedFile = path.resolve("shared-1");
        final UUID backendIdentifier = UUID.randomUUID();
        final CompletedCheckpoint initialSavepoint =
                createInitialSavepoint(location, job, sharedFile, backendIdentifier);

        initialSavepoint.registerSharedStatesAfterRestored(stateRegistry);
        addCheckpointAndSubsume(checkpointStore, initialSavepoint);
        checkpointStore.deleteLocationWhenEmpty(location);

        final CompletedCheckpoint reusingCheckpoint =
                createReusingSharedFilesCheckpoint(job, sharedFile.toString(), backendIdentifier);
        stateRegistry.registerAll(
                reusingCheckpoint.getOperatorStates().values(),
                reusingCheckpoint.getCheckpointID());
        addCheckpointAndSubsume(checkpointStore, reusingCheckpoint);
        Assertions.assertThat(path).exists();

        final CompletedCheckpoint independentCheckpoint1 = createIndependentCheckpoint(job, 2L);
        stateRegistry.registerAll(
                independentCheckpoint1.getOperatorStates().values(),
                independentCheckpoint1.getCheckpointID());
        addCheckpointAndSubsume(checkpointStore, independentCheckpoint1);

        // shared state is unregistered after a checkpoint is disposed, at the same time we
        // piggyback on the async subsume process, thus the actual deletion happens one checkpoint
        // after the last checkpoint that shared the last file
        final CompletedCheckpoint independentCheckpoint2 = createIndependentCheckpoint(job, 3L);
        stateRegistry.registerAll(
                independentCheckpoint2.getOperatorStates().values(),
                independentCheckpoint2.getCheckpointID());
        addCheckpointAndSubsume(checkpointStore, independentCheckpoint2);
        Assertions.assertThat(path).doesNotExist();
    }

    private void addCheckpointAndSubsume(
            CompletedCheckpointStore checkpointStore, CompletedCheckpoint checkpoint)
            throws Exception {
        try (final CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner()) {
            checkpointStore.addCheckpointAndSubsumeOldestOne(
                    checkpoint, checkpointsCleaner, () -> {});
        }
    }

    @NotNull
    private CompletedCheckpoint createInitialSavepoint(
            FsCompletedCheckpointStorageLocation location,
            JobID job,
            Path sharedFile,
            UUID backendIdentifier)
            throws IOException {

        final Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        final OperatorID operatorID = new OperatorID();
        final OperatorState operatorState = new OperatorState(operatorID, 1, 1);
        final HashMap<StateHandleID, StreamStateHandle> sharedState = new HashMap<>();
        Files.write(sharedFile, new byte[] {});
        sharedState.put(
                new StateHandleID(sharedFile.toString()),
                new RelativeFileStateHandle(
                        new org.apache.flink.core.fs.Path(sharedFile.toUri()),
                        sharedFile.getFileName().toString(),
                        0));
        final OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                new IncrementalRemoteKeyedStateHandle(
                                        backendIdentifier,
                                        KeyGroupRange.of(0, 1),
                                        0L,
                                        sharedState,
                                        emptyMap(),
                                        new EmptyStreamStateHandle()))
                        .build();
        operatorState.putState(0, subtaskState);
        operatorStates.put(operatorID, operatorState);
        return new CompletedCheckpoint(
                job,
                0L,
                0L,
                0L,
                operatorStates,
                emptyList(),
                CheckpointProperties.forSavepoint(false, SavepointFormatType.NATIVE),
                location);
    }

    @NotNull
    private CompletedCheckpoint createReusingSharedFilesCheckpoint(
            JobID job, String reusedFileName, UUID backendIdentifier) {

        final Map<OperatorID, OperatorState> operatorStates = new HashMap<>();
        final OperatorID operatorID = new OperatorID();
        final OperatorState operatorState = new OperatorState(operatorID, 1, 1);
        final HashMap<StateHandleID, StreamStateHandle> sharedState = new HashMap<>();
        sharedState.put(new StateHandleID(reusedFileName), new PlaceholderStreamStateHandle(0));
        final OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                new IncrementalRemoteKeyedStateHandle(
                                        backendIdentifier,
                                        KeyGroupRange.of(0, 1),
                                        0L,
                                        sharedState,
                                        emptyMap(),
                                        new EmptyStreamStateHandle()))
                        .build();
        operatorState.putState(0, subtaskState);
        operatorStates.put(operatorID, operatorState);
        return new CompletedCheckpoint(
                job,
                1L,
                0L,
                0L,
                operatorStates,
                emptyList(),
                CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION),
                new TestCompletedCheckpointStorageLocation());
    }

    @NotNull
    private CompletedCheckpoint createIndependentCheckpoint(JobID job, long checkpointID) {
        return new CompletedCheckpoint(
                job,
                checkpointID,
                0L,
                0L,
                emptyMap(),
                emptyList(),
                CheckpointProperties.forCheckpoint(NEVER_RETAIN_AFTER_TERMINATION),
                new TestCompletedCheckpointStorageLocation());
    }

    private static CompletedCheckpointStore createDefaultCompletedCheckpointStore(
            TestingStateHandleStore<CompletedCheckpoint> stateHandleStore) throws Exception {
        final CheckpointStoreUtil checkpointStoreUtil =
                new CheckpointStoreUtil() {

                    @Override
                    public String checkpointIDToName(long checkpointId) {
                        return String.valueOf(checkpointId);
                    }

                    @Override
                    public long nameToCheckpointID(String name) {
                        return Long.parseLong(name);
                    }
                };
        return new DefaultCompletedCheckpointStore<>(
                1,
                stateHandleStore,
                checkpointStoreUtil,
                DefaultCompletedCheckpointStoreUtils.retrieveCompletedCheckpoints(
                        stateHandleStore, checkpointStoreUtil),
                SharedStateRegistry.DEFAULT_FACTORY.create(
                        org.apache.flink.util.concurrent.Executors.directExecutor(), emptyList()),
                org.apache.flink.util.concurrent.Executors.directExecutor());
    }
}
