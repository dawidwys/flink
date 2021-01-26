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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeapStateKeyGroupMergeIteratorTest {
    @Test
    public void testValueState() throws IOException {

        IntSerializer keySerializer = IntSerializer.INSTANCE;
        StringSerializer namespaceSerializer = StringSerializer.INSTANCE;
        LongSerializer stateSerializer = LongSerializer.INSTANCE;

        RegisteredKeyValueStateBackendMetaInfo<String, Long> valueStateMetaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE, "test", namespaceSerializer, stateSerializer);

        RegisteredKeyValueStateBackendMetaInfo<String, List<Long>> listStateMetaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.LIST, "test", namespaceSerializer,
                        new ListSerializer<>(stateSerializer));

        KeyGroupRange keyGroupRange = new KeyGroupRange(0, 3);
        int totalKeyGroups = 4;
        MockInternalKeyContext<Integer> mockKeyContext = new MockInternalKeyContext<>(
                keyGroupRange.getStartKeyGroup(),
                keyGroupRange.getEndKeyGroup(),
                totalKeyGroups);
        CopyOnWriteStateTable<Integer, String, Long> valueTable =
                new CopyOnWriteStateTable<>(mockKeyContext, valueStateMetaInfo, keySerializer);

        CopyOnWriteStateTable<Integer, String, List<Long>> listTable =
                new CopyOnWriteStateTable<>(mockKeyContext, listStateMetaInfo, keySerializer);

        RegisteredPriorityQueueStateBackendMetaInfo<TimerHeapInternalTimer<Integer, String>> queueMetaInfo =
                new RegisteredPriorityQueueStateBackendMetaInfo<>(
                "queue",
                new TimerSerializer<>(keySerializer, namespaceSerializer)
        );
        final HeapPriorityQueueSet<TimerHeapInternalTimer<Integer, String>> priorityQueue =
                new HeapPriorityQueueSetFactory(
                        keyGroupRange,
                        totalKeyGroups,
                        2
                ).create(queueMetaInfo.getName(), queueMetaInfo.getElementSerializer());

        HeapPriorityQueueSnapshotRestoreWrapper<TimerHeapInternalTimer<Integer, String>> wrapper =
                new HeapPriorityQueueSnapshotRestoreWrapper<>(
                        priorityQueue,
                        queueMetaInfo,
                        KeyExtractorFunction.forKeyedObjects(),
                        keyGroupRange,
                        totalKeyGroups);

        mockKeyContext.setCurrentKeyAndKeyGroup(0);
        valueTable.put("a", 0L);
        valueTable.put("b", 1L);
        mockKeyContext.setCurrentKeyAndKeyGroup(1);
        valueTable.put("a", 2L);
        valueTable.put("b", 3L);
        mockKeyContext.setCurrentKeyAndKeyGroup(2);
        valueTable.put("a", 4L);
        valueTable.put("b", 5L);

        mockKeyContext.setCurrentKeyAndKeyGroup(0);
        listTable.put("a", Arrays.asList(0L, 1L));
        listTable.put("b", Arrays.asList(2L, 3L));
        mockKeyContext.setCurrentKeyAndKeyGroup(1);
        listTable.put("a", Arrays.asList(4L, 5L));
        listTable.put("b", Arrays.asList(6L, 7L));
        mockKeyContext.setCurrentKeyAndKeyGroup(2);
        listTable.put("a", Arrays.asList(8L, 9L));
        listTable.put("b", Arrays.asList(10L, 11L));

        priorityQueue.add(new TimerHeapInternalTimer<>(0L, 0, "a"));
        priorityQueue.add(new TimerHeapInternalTimer<>(1L, 0, "b"));
        priorityQueue.add(new TimerHeapInternalTimer<>(2L, 1, "a"));
        priorityQueue.add(new TimerHeapInternalTimer<>(3L, 1, "b"));
        priorityQueue.add(new TimerHeapInternalTimer<>(4L, 2, "a"));
        priorityQueue.add(new TimerHeapInternalTimer<>(5L, 2, "b"));

        StateUID valueStateUID = StateUID.of("value", StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
        StateUID listStateUID = StateUID.of("list", StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
        StateUID queueStateUID = StateUID.of("queue", StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);
        Map<StateUID, Integer> stateNamesToId = new HashMap<>();
        stateNamesToId.put(valueStateUID, 0);
        stateNamesToId.put(listStateUID, 1);
        stateNamesToId.put(queueStateUID, 2);

        Map<StateUID, StateSnapshot> cowStateTables = new HashMap<>();
        cowStateTables.put(valueStateUID, valueTable.stateSnapshot());
        cowStateTables.put(listStateUID, listTable.stateSnapshot());
        cowStateTables.put(queueStateUID, wrapper.stateSnapshot());

        HeapStateKeyGroupMergeIterator iterator =
                new HeapStateKeyGroupMergeIterator(
                        keyGroupRange,
                        keySerializer,
                        totalKeyGroups,
                        stateNamesToId,
                        cowStateTables);

        while (iterator.isValid()) {
            if (iterator.isNewKeyGroup()) {
                System.out.println("New key group: " + iterator.keyGroup());
            }
            if (iterator.isNewKeyValueState()) {
                System.out.println("New kv state: " + iterator.kvStateId());
            }
            System.out.println(Arrays.toString(iterator.key()));
            System.out.println(Arrays.toString(iterator.value()));
            iterator.next();
        }
    }
}
