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

package org.apache.flink.test.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBTestUtils;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapStateBackendTestBase;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.TimerSerializer;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class SavepointStateBackendSwitchTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void switchFromRocksToHeap() throws Exception {

        final File pathToWrite = tempFolder.newFile("tmp_rocks_map_state");

        final MapStateDescriptor<Long, Long> stateDescr = new MapStateDescriptor<>("my-map-state",
                Long.class, Long.class);
        stateDescr.initializeSerializerUnlessSet(new ExecutionConfig());

        final ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<Long>(
                "my-value-state",
                Long.class
        );
        valueStateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());

        final Integer namespace1 = 1;
        final Integer namespace2 = 2;
        final Integer namespace3 = 3;

        takeRocksSavepoint(
                pathToWrite,
                stateDescr,
                valueStateDescriptor,
                namespace1,
                namespace2,
                namespace3);

        final SnapshotResult<KeyedStateHandle> stateHandles;
        try (
                BufferedInputStream bis =
                        new BufferedInputStream((new FileInputStream(pathToWrite)))) {
            stateHandles =
                    InstantiationUtil.deserializeObject(
                            bis, Thread.currentThread().getContextClassLoader());
        }
        final KeyedStateHandle stateHandle = stateHandles.getJobManagerOwnedSnapshot();
        try (
                final HeapKeyedStateBackend<String> keyedBackend =
                        createHeapKeyedStateBackend(StateObjectCollection.singleton(stateHandle))) {

            InternalMapState<String, Integer, Long, Long> state =
                    keyedBackend.createInternalState(IntSerializer.INSTANCE, stateDescr);

            InternalValueState<String, Integer, Long> valueState =
                    keyedBackend.createInternalState(IntSerializer.INSTANCE, valueStateDescriptor);

            keyedBackend.setCurrentKey("abc");
            state.setCurrentNamespace(namespace1);
            assertEquals(33L, (long) state.get(33L));
            assertEquals(55L, (long) state.get(55L));
            assertEquals(2, getStateSize(state));

            state.setCurrentNamespace(namespace2);
            assertEquals(22L, (long) state.get(22L));
            assertEquals(11L, (long) state.get(11L));
            assertEquals(2, getStateSize(state));

            state.setCurrentNamespace(namespace3);
            assertEquals(44L, (long) state.get(44L));
            assertEquals(1, getStateSize(state));

            keyedBackend.setCurrentKey("mno");
            state.setCurrentNamespace(namespace3);
            assertEquals(11L, (long) state.get(11L));
            assertEquals(22L, (long) state.get(22L));
            assertEquals(33L, (long) state.get(33L));
            assertEquals(44L, (long) state.get(44L));
            assertEquals(55L, (long) state.get(55L));
            assertEquals(5, getStateSize(state));
            valueState.setCurrentNamespace(namespace3);
            assertEquals(1239L, (long) valueState.value());

            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<String, Integer>> priorityQueue
                    = keyedBackend.create("event-time", new TimerSerializer<>(
                    keyedBackend.getKeySerializer(),
                    IntSerializer.INSTANCE
            ));

            assertThat(priorityQueue.size(), equalTo(1));
            assertThat(
                    priorityQueue.poll(),
                    equalTo(new TimerHeapInternalTimer<>(1234L, "mno", namespace3)));
        }
    }

    private HeapKeyedStateBackend<String> createHeapKeyedStateBackend(Collection<KeyedStateHandle> stateHandles) throws BackendBuildingException {
        final KeyGroupRange keyGroupRange = new KeyGroupRange(0, 1);
        final int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();
        ExecutionConfig executionConfig = new ExecutionConfig();

        return new HeapKeyedStateBackendBuilder<>(
                mock(TaskKvStateRegistry.class),
                StringSerializer.INSTANCE,
                HeapStateBackendTestBase.class.getClassLoader(),
                numKeyGroups,
                keyGroupRange,
                executionConfig,
                TtlTimeProvider.DEFAULT,
                stateHandles,
                AbstractStateBackend.getCompressionDecorator(executionConfig),
                TestLocalRecoveryConfig.disabled(),
                new HeapPriorityQueueSetFactory(keyGroupRange, numKeyGroups, 128),
                true,
                new CloseableRegistry())
                .build(true);
    }

    private <K, N, UK, UV> int getStateSize(InternalMapState<K, N, UK, UV> mapState)
            throws Exception {
        int i = 0;
        Iterator<Map.Entry<UK, UV>> itt = mapState.iterator();
        while (itt.hasNext()) {
            i++;
            itt.next();
        }
        return i;
    }

    private void takeRocksSavepoint(
            File pathToWrite,
            MapStateDescriptor<Long, Long> stateDescr,
            ValueStateDescriptor<Long> valueStateDescriptor,
            Integer namespace1,
            Integer namespace2,
            Integer namespace3) throws Exception {
        try (
                final RocksDBKeyedStateBackend<String> keyedBackend = RocksDBTestUtils
                        .builderForTestDefaults(
                                tempFolder.newFolder(),
                                StringSerializer.INSTANCE,
                                RocksDBStateBackend.PriorityQueueStateType.ROCKSDB)
                        .build()) {
            InternalMapState<String, Integer, Long, Long> mapState =
                    keyedBackend.createInternalState(IntSerializer.INSTANCE, stateDescr);

            InternalValueState<String, Integer, Long> valueState = keyedBackend.createInternalState(
                    IntSerializer.INSTANCE,
                    valueStateDescriptor);

            keyedBackend.setCurrentKey("abc");
            mapState.setCurrentNamespace(namespace1);
            mapState.put(33L, 33L);
            mapState.put(55L, 55L);

            mapState.setCurrentNamespace(namespace2);
            mapState.put(22L, 22L);
            mapState.put(11L, 11L);

            mapState.setCurrentNamespace(namespace3);
            mapState.put(44L, 44L);

            keyedBackend.setCurrentKey("mno");
            mapState.setCurrentNamespace(namespace3);
            mapState.put(11L, 11L);
            mapState.put(22L, 22L);
            mapState.put(33L, 33L);
            mapState.put(44L, 44L);
            mapState.put(55L, 55L);
            valueState.setCurrentNamespace(namespace3);
            valueState.update(1239L);

            KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<String, Integer>> priorityQueue
                    = keyedBackend.create("event-time", new TimerSerializer<>(
                    keyedBackend.getKeySerializer(),
                    IntSerializer.INSTANCE
            ));

            priorityQueue.add(new TimerHeapInternalTimer<>(1234L, "mno", namespace3));

            RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot = keyedBackend.snapshot(
                    0L,
                    0L,
                    new MemCheckpointStreamFactory(4 * 1024 * 1024),
                    CheckpointOptions.forCheckpointWithDefaultLocation());

            snapshot.run();

            try (
                    BufferedOutputStream bis = new BufferedOutputStream(new
                            FileOutputStream(pathToWrite))) {
                InstantiationUtil.serializeObject(bis, snapshot.get());
            }
        }
    }
}
