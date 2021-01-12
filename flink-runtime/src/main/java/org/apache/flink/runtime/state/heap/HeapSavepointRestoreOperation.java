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
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.restore.ExceptionIterator;
import org.apache.flink.runtime.state.restore.KeyGroup;
import org.apache.flink.runtime.state.restore.KeyGroupEntry;
import org.apache.flink.runtime.state.restore.SavepointRestoreOperation;
import org.apache.flink.runtime.state.restore.SavepointRestoreResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.restore.SnapshotUtil.computeRequiredBytesInKeyGroupPrefix;
import static org.apache.flink.runtime.state.restore.SnapshotUtil.readKey;
import static org.apache.flink.runtime.state.restore.SnapshotUtil.readKeyGroup;
import static org.apache.flink.runtime.state.restore.SnapshotUtil.readNamespace;

/**
 * Implementation of heap restore operation.
 *
 * @param <K> The data type that the serializer serializes.
 */
public class HeapSavepointRestoreOperation<K> implements RestoreOperation<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(HeapSavepointRestoreOperation.class);
    private final StateSerializerProvider<K> keySerializerProvider;
    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final SavepointRestoreOperation<K> savepointRestoreOperation;
    private final int keyGroupPrefixBytes;
    private final HeapMetaInfoRestoreOperation<K> heapMetaInfoRestoreOperation;

    HeapSavepointRestoreOperation(
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            StateSerializerProvider<K> keySerializerProvider,
            ClassLoader userCodeClassLoader,
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            CloseableRegistry cancelStreamRegistry,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            @Nonnull KeyGroupRange keyGroupRange,
            int numberOfKeyGroups,
            HeapSnapshotStrategy<K> snapshotStrategy,
            InternalKeyContext<K> keyContext) {
        this.keySerializerProvider = keySerializerProvider;
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.savepointRestoreOperation = new SavepointRestoreOperation<>(
                keyGroupRange,
                cancelStreamRegistry,
                userCodeClassLoader,
                restoreStateHandles,
                keySerializerProvider
        );
        this.keyGroupPrefixBytes = computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);
        this.heapMetaInfoRestoreOperation = new HeapMetaInfoRestoreOperation<>(
                keySerializerProvider,
                priorityQueueSetFactory,
                keyGroupRange,
                numberOfKeyGroups,
                snapshotStrategy,
                keyContext
        );
    }

    @Override
    public Void restore() throws Exception {

        registeredKVStates.clear();
        registeredPQStates.clear();

        List<SavepointRestoreResult> restore = this.savepointRestoreOperation.restore();
        for (SavepointRestoreResult restoreResult : restore) {
            List<StateMetaInfoSnapshot> restoredMetaInfos =
                    restoreResult.getStateMetaInfoSnapshots();

            final Map<Integer, StateMetaInfoSnapshot> kvStatesById =
                    this.heapMetaInfoRestoreOperation.createOrCheckStateForMetaInfo(
                            restoredMetaInfos,
                            registeredKVStates,
                            registeredPQStates);

            ExceptionIterator<KeyGroup> keyGroups = restoreResult.getRestoredKeyGroups();
            while (keyGroups.hasNext()) {
                readKeyGroupStateData(
                        keyGroups.next(),
                        keySerializerProvider.previousSchemaSerializer(),
                        kvStatesById
                );
            }
        }

        return null;
    }

    private void readKeyGroupStateData(
            KeyGroup keyGroup,
            TypeSerializer<K> keySerializer,
            Map<Integer, StateMetaInfoSnapshot> kvStatesById)
            throws Exception {

        ExceptionIterator<KeyGroupEntry> entries = keyGroup.getKeyGroupEntries();
        while (entries.hasNext()) {
            KeyGroupEntry groupEntry = entries.next();
            StateMetaInfoSnapshot infoSnapshot = kvStatesById.get(groupEntry.getKvStateId());
            switch (infoSnapshot.getBackendStateType()) {
                case KEY_VALUE:
                    readKVStateData(keySerializer, groupEntry, infoSnapshot);
                    break;
                case PRIORITY_QUEUE:
                    readPriorityQueue(groupEntry, infoSnapshot);
                    break;
                case OPERATOR:
                case BROADCAST:
                    throw new IllegalStateException("Expected only keyed state. Received: "
                            + infoSnapshot.getBackendStateType());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readPriorityQueue(
            KeyGroupEntry groupEntry,
            StateMetaInfoSnapshot infoSnapshot) throws IOException {
        DataInputDeserializer keyDeserializer = new DataInputDeserializer(groupEntry.getKey());
        keyDeserializer.skipBytesToRead(keyGroupPrefixBytes);
        HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement> priorityQueueSnapshotRestoreWrapper =
                (HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement>) registeredPQStates
                        .get(infoSnapshot.getName());
        HeapPriorityQueueElement timer = priorityQueueSnapshotRestoreWrapper
                .getMetaInfo()
                .getElementSerializer()
                .deserialize(keyDeserializer);
        HeapPriorityQueueSet<HeapPriorityQueueElement> priorityQueue = priorityQueueSnapshotRestoreWrapper
                .getPriorityQueue();
        priorityQueue.add(timer);
    }

    @SuppressWarnings("unchecked")
    private void readKVStateData(
            TypeSerializer<K> keySerializer,
            KeyGroupEntry groupEntry,
            StateMetaInfoSnapshot infoSnapshot) throws IOException {
        StateTable<K, Object, Object> stateTable = (StateTable<K, Object, Object>) registeredKVStates
                .get(infoSnapshot.getName());
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo = stateTable.getMetaInfo();
        TypeSerializer<?> namespaceSerializer = metaInfo.getPreviousNamespaceSerializer();
        boolean isAmbigousKey =
                keySerializer.getLength() < 0 && namespaceSerializer.getLength() < 0;
        byte[] keyBytes = groupEntry.getKey();
        DataInputDeserializer keyDeserializer = new DataInputDeserializer(keyBytes);
        DataInputDeserializer valueDeserializer = new DataInputDeserializer(groupEntry.getValue());
        int keyGroup = readKeyGroup(keyGroupPrefixBytes, keyDeserializer);
        K key = readKey(keySerializer, keyDeserializer, isAmbigousKey);
        Object namespace = readNamespace(
                namespaceSerializer,
                keyDeserializer,
                isAmbigousKey);
        TypeSerializer<?> stateSerializer = metaInfo.getPreviousStateSerializer();
        switch (metaInfo.getStateType()) {
            case VALUE:
            case LIST:
            case REDUCING:
            case FOLDING:
            case AGGREGATING: {
                stateTable.put(
                        key,
                        keyGroup,
                        namespace,
                        stateSerializer.deserialize(valueDeserializer));
            }
            break;
            case MAP:
                MapSerializer<Object, Object> mapSerializer = (MapSerializer<Object, Object>) stateSerializer;
                Object mapEntryKey = mapSerializer.getKeySerializer().deserialize(keyDeserializer);
                boolean isNull = valueDeserializer.readBoolean();
                final Object mapEntryValue;
                if (isNull) {
                    mapEntryValue = null;
                } else {
                    mapEntryValue = mapSerializer.getValueSerializer()
                            .deserialize(valueDeserializer);
                }
                Map<Object, Object> userMap = (Map<Object, Object>) stateTable.get(
                        key,
                        namespace);
                if (userMap == null) {
                    userMap = new HashMap<>();
                    stateTable.put(key, keyGroup, namespace, userMap);
                }
                userMap.put(mapEntryKey, mapEntryValue);
                break;
            case UNKNOWN:
            default:
                throw new IllegalStateException("Unknown state type");
        }
    }
}
