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
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeapStateKeyGroupMergeIteratorTest {
    @Test
    public void testValueState() throws IOException {

        StringSerializer keySerializer = StringSerializer.INSTANCE;
        IntSerializer namespaceSerializer = IntSerializer.INSTANCE;
        LongSerializer stateSerializer = LongSerializer.INSTANCE;

        RegisteredKeyValueStateBackendMetaInfo<Integer, Long> valueStateMetaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE, "test", namespaceSerializer, stateSerializer);

        RegisteredKeyValueStateBackendMetaInfo<Integer, List<Long>> listStateMetaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.LIST, "test", namespaceSerializer,
                        new ListSerializer<>(stateSerializer));

        InternalKeyContext<String> mockKeyContext = new MockInternalKeyContext<>(0, 2, 4);
        CopyOnWriteStateTable<String, Integer, Long> valueTable =
                new CopyOnWriteStateTable<>(mockKeyContext, valueStateMetaInfo, keySerializer);

        CopyOnWriteStateTable<String, Integer, List<Long>> listTable =
                new CopyOnWriteStateTable<>(mockKeyContext, listStateMetaInfo, keySerializer);

        valueTable.put("a", 0, 0, 0L);
        valueTable.put("a", 0, 1, 1L);
        valueTable.put("b", 1, 0, 2L);
        valueTable.put("b", 1, 1, 3L);
        valueTable.put("c", 2, 0, 4L);
        valueTable.put("c", 2, 1, 5L);

        listTable.put("a", 0, 0, Arrays.asList(0L, 1L));
        listTable.put("a", 0, 1, Arrays.asList(2L, 3L));
        listTable.put("b", 1, 0, Arrays.asList(4L, 5L));
        listTable.put("b", 1, 1, Arrays.asList(6L, 7L));
        listTable.put("c", 2, 0, Arrays.asList(8L, 9L));
        listTable.put("c", 2, 1, Arrays.asList(10L, 11L));

        StateUID valueStateUID = StateUID.of("value", StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
        StateUID listStateUID = StateUID.of("list", StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);
        Map<StateUID, Integer> stateNamesToId = new HashMap<>();
        stateNamesToId.put(valueStateUID, 0);
        stateNamesToId.put(listStateUID, 1);

        Map<StateUID, IterableStateSnapshot<?, ?, ?>> cowStateTables = new HashMap<>();
        cowStateTables.put(valueStateUID, valueTable.stateSnapshot());
        cowStateTables.put(listStateUID, listTable.stateSnapshot());

        HeapStateKeyGroupMergeIterator iterator =
                new HeapStateKeyGroupMergeIterator(
                        new KeyGroupRange(0, 2),
                        keySerializer,
                        4,
                        stateNamesToId,
                        cowStateTables,
                        Collections.emptyMap());

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
