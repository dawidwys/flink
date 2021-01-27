/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.FullSnapshotAsyncWriter;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A strategy how to perform a snapshot of a {@link HeapKeyedStateBackend}. */
class HeapSavepointStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, FullSnapshotResources<K>> {

    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;
    private final StreamCompressionDecorator keyGroupCompressionDecorator;
    private final LocalRecoveryConfig localRecoveryConfig;
    private final KeyGroupRange keyGroupRange;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final int totalKeyGroups;

    HeapSavepointStrategy(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            KeyGroupRange keyGroupRange,
            StateSerializerProvider<K> keySerializerProvider,
            int totalKeyGroups) {
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.localRecoveryConfig = localRecoveryConfig;
        this.keyGroupRange = keyGroupRange;
        this.keySerializerProvider = keySerializerProvider;
        this.totalKeyGroups = totalKeyGroups;
    }

    @Override
    public FullSnapshotResources<K> syncPrepareResources(long checkpointId) {

        if (!hasRegisteredState()) {
            return new HeapSavepointResources<>(
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    keyGroupCompressionDecorator,
                    Collections.emptyMap(),
                    keyGroupRange,
                    keySerializerProvider.previousSchemaSerializer(),
                    totalKeyGroups);
        }

        int numStates = registeredKVStates.size() + registeredPQStates.size();

        Preconditions.checkState(
                numStates <= Short.MAX_VALUE,
                "Too many states: "
                        + numStates
                        + ". Currently at most "
                        + Short.MAX_VALUE
                        + " states are supported");

        final List<StateMetaInfoSnapshot> metaInfoSnapshots = new ArrayList<>(numStates);
        final Map<StateUID, Integer> stateNamesToId = new HashMap<>(numStates);
        final Map<StateUID, StateSnapshot> cowStateStableSnapshots = new HashMap<>(numStates);

        processSnapshotMetaInfoForAllStates(
                metaInfoSnapshots,
                cowStateStableSnapshots,
                stateNamesToId,
                registeredKVStates,
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);

        processSnapshotMetaInfoForAllStates(
                metaInfoSnapshots,
                cowStateStableSnapshots,
                stateNamesToId,
                registeredPQStates,
                StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);

        return new HeapSavepointResources<>(
                metaInfoSnapshots,
                cowStateStableSnapshots,
                keyGroupCompressionDecorator,
                stateNamesToId,
                keyGroupRange,
                getKeySerializer(),
                totalKeyGroups);
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            FullSnapshotResources<K> syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {
        return new FullSnapshotAsyncWriter<>(
                createCheckpointStreamSupplier(checkpointId, streamFactory, checkpointOptions),
                syncPartResource
        );
    }

    private void processSnapshotMetaInfoForAllStates(
            List<StateMetaInfoSnapshot> metaInfoSnapshots,
            Map<StateUID, StateSnapshot> cowStateStableSnapshots,
            Map<StateUID, Integer> stateNamesToId,
            Map<String, ? extends StateSnapshotRestore> registeredStates,
            StateMetaInfoSnapshot.BackendStateType stateType) {

        for (Map.Entry<String, ? extends StateSnapshotRestore> kvState :
                registeredStates.entrySet()) {
            final StateUID stateUid = StateUID.of(kvState.getKey(), stateType);
            stateNamesToId.put(stateUid, stateNamesToId.size());
            StateSnapshotRestore state = kvState.getValue();
            if (null != state) {
                final StateSnapshot stateSnapshot = state.stateSnapshot();
                metaInfoSnapshots.add(stateSnapshot.getMetaInfoSnapshot());
                cowStateStableSnapshots.put(stateUid, stateSnapshot);
            }
        }
    }

    private SupplierWithException<CheckpointStreamWithResultProvider, Exception>
            createCheckpointStreamSupplier(
                    long checkpointId,
                    CheckpointStreamFactory primaryStreamFactory,
                    CheckpointOptions checkpointOptions) {

        return localRecoveryConfig.isLocalRecoveryEnabled()
                        && !checkpointOptions.getCheckpointType().isSavepoint()
                ? () ->
                        CheckpointStreamWithResultProvider.createDuplicatingStream(
                                checkpointId,
                                CheckpointedStateScope.EXCLUSIVE,
                                primaryStreamFactory,
                                localRecoveryConfig.getLocalStateDirectoryProvider())
                : () ->
                        CheckpointStreamWithResultProvider.createSimpleStream(
                                CheckpointedStateScope.EXCLUSIVE, primaryStreamFactory);
    }

    private boolean hasRegisteredState() {
        return !(registeredKVStates.isEmpty() && registeredPQStates.isEmpty());
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializerProvider.currentSchemaSerializer();
    }

    static class HeapSavepointResources<K> implements FullSnapshotResources<K> {
        private final List<StateMetaInfoSnapshot> metaInfoSnapshots;
        private final Map<StateUID, StateSnapshot> cowStateStableSnapshots;
        private final StreamCompressionDecorator streamCompressionDecorator;
        private final Map<StateUID, Integer> stateNamesToId;
        private final KeyGroupRange keyGroupRange;
        private final TypeSerializer<K> keySerializer;
        private final int totalKeyGroups;

        HeapSavepointResources(
                List<StateMetaInfoSnapshot> metaInfoSnapshots,
                Map<StateUID, StateSnapshot> cowStateStableSnapshots,
                StreamCompressionDecorator streamCompressionDecorator,
                Map<StateUID, Integer> stateNamesToId,
                KeyGroupRange keyGroupRange,
                TypeSerializer<K> keySerializer,
                int totalKeyGroups) {
            this.metaInfoSnapshots = metaInfoSnapshots;
            this.cowStateStableSnapshots = cowStateStableSnapshots;
            this.streamCompressionDecorator = streamCompressionDecorator;
            this.stateNamesToId = stateNamesToId;
            this.keyGroupRange = keyGroupRange;
            this.keySerializer = keySerializer;
            this.totalKeyGroups = totalKeyGroups;
        }

        @Override
        public void release() {
            for (StateSnapshot stateSnapshot : cowStateStableSnapshots.values()) {
                stateSnapshot.release();
            }
        }

        public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
            return metaInfoSnapshots;
        }

        @Override
        public KeyValueStateIterator createKVStateIterator() throws IOException {
            return new HeapStateKeyGroupMergeIterator(
                    keyGroupRange,
                    keySerializer,
                    totalKeyGroups,
                    stateNamesToId,
                    cowStateStableSnapshots
            );
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            return keyGroupRange;
        }

        @Override
        public TypeSerializer<K> getKeySerializer() {
            return keySerializer;
        }

        @Override
        public StreamCompressionDecorator getStreamCompressionDecorator() {
            return streamCompressionDecorator;
        }

    }
}
