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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.IterableStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SavepointSerializationUtils;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Javadoc. */
@Internal
public class HeapStateKeyGroupMergeIterator implements KeyValueStateIterator {

    private final Map<StateUID, Integer> stateNamesToId;
    private final Map<StateUID, StateSnapshot> cowStateStableSnapshots;
    private final TypeSerializer<?> keySerializer;
    private final int keyGroupPrefixBytes;
    private final Iterator<Integer> keyGroupIterator;

    private Iterator<StateUID> statesIterator;
    private boolean isValid;
    private boolean newKeyGroup;
    private boolean newKVState;

    private int currentKeyGroup;

    public HeapStateKeyGroupMergeIterator(
            final KeyGroupRange keyGroupRange,
            final TypeSerializer<?> keySerializer,
            final int totalKeyGroups,
            final Map<StateUID, Integer> stateNamesToId,
            final Map<StateUID, StateSnapshot> stateSnapshots)
            throws IOException {
        checkNotNull(keyGroupRange);
        this.stateNamesToId = checkNotNull(stateNamesToId);
        this.cowStateStableSnapshots = checkNotNull(stateSnapshots);

        this.statesIterator = stateSnapshots.keySet().iterator();
        this.keyGroupIterator = keyGroupRange.iterator();
        this.keySerializer = keySerializer;

        this.keyGroupPrefixBytes =
                SavepointSerializationUtils.computeRequiredBytesInKeyGroupPrefix(totalKeyGroups);

        if (!keyGroupIterator.hasNext() || !statesIterator.hasNext()) {
            // stop early, no key groups or states
            isValid = false;
        } else {
            currentKeyGroup = keyGroupIterator.next();
            next();
            this.newKeyGroup = true;
        }
    }

    @Override
    public boolean isValid() {
        return isValid;
    }

    @Override
    public boolean isNewKeyValueState() {
        return this.newKVState;
    }

    @Override
    public boolean isNewKeyGroup() {
        return this.newKeyGroup;
    }

    @Override
    public int keyGroup() {
        return currentKeyGroup;
    }

    @Override
    public int kvStateId() {
        return stateNamesToId.get(currentState);
    }

    private StateUID currentState;
    private SingleStateIterator currentStateIterator;

    private final DataOutputSerializer keyOut = new DataOutputSerializer(64);
    private final DataOutputSerializer valueOut = new DataOutputSerializer(64);

    @Override
    public void next() throws IOException {
        boolean hasStateEntry = false;

        do {
            if (currentState == null) {
                boolean hasNextState = moveToNextState();
                if (!hasNextState) {
                    break;
                }
            } else {
                this.newKVState = false;
                this.newKeyGroup = false;
            }

            hasStateEntry = currentStateIterator != null && currentStateIterator.hasNext();
            if (hasStateEntry) {
                break;
            } else {
                this.currentState = null;
            }
        } while (true);

        if (hasStateEntry) {
            isValid = true;
            currentStateIterator.writeOutNext();
        } else {
            isValid = false;
        }
    }

    private boolean moveToNextState() {
        if (statesIterator.hasNext()) {
            this.currentState = statesIterator.next();
            this.newKVState = true;
            this.newKeyGroup = false;
        } else if (keyGroupIterator.hasNext()) {
            this.currentKeyGroup = keyGroupIterator.next();
            this.statesIterator = cowStateStableSnapshots.keySet().iterator();
            this.currentState = statesIterator.next();
            this.newKeyGroup = true;
            this.newKVState = true; // TODO what if only a single state?
        } else {
            return false;
        }

        StateSnapshot stateSnapshot = this.cowStateStableSnapshots.get(currentState);
        if (stateSnapshot instanceof IterableStateSnapshot) {
            this.currentStateIterator = new StateTableIterator(
                    ((IterableStateSnapshot<?, ?, ?>) stateSnapshot).getIterator(currentKeyGroup),
                    new RegisteredKeyValueStateBackendMetaInfo<>(stateSnapshot.getMetaInfoSnapshot())
            );
        } else if (stateSnapshot instanceof HeapPriorityQueueStateSnapshot) {
            this.currentStateIterator = new QueueIterator(
                    new RegisteredPriorityQueueStateBackendMetaInfo<>(stateSnapshot.getMetaInfoSnapshot())
            );
        }

        // set to a valid entry
        return true;
    }

    private interface SingleStateIterator {

        boolean hasNext();

        void writeOutNext() throws IOException;
    }

    private class StateTableIterator implements SingleStateIterator {

        private final Iterator<? extends StateEntry<?, ?, ?>> currentStateIterator;
        private final RegisteredKeyValueStateBackendMetaInfo<?, ?> currentStateSnapshot;

        private StateTableIterator(
                Iterator<? extends StateEntry<?, ?, ?>> currentStateIterator,
                RegisteredKeyValueStateBackendMetaInfo<?, ?> currentStateSnapshot) {
            this.currentStateIterator = currentStateIterator;
            this.currentStateSnapshot = currentStateSnapshot;
        }

        @Override
        public boolean hasNext() {
            return currentStateIterator.hasNext();
        }

        @Override
        public void writeOutNext() throws IOException {
            StateEntry<?, ?, ?> currentEntry = currentStateIterator.next();
            keyOut.clear();
            valueOut.clear();
            // set the values we need to set
            boolean isAmbigousKeyPossible =
                    SavepointSerializationUtils.isAmbiguousKeyPossible(
                            keySerializer, currentStateSnapshot.getPreviousNamespaceSerializer());
            SavepointSerializationUtils.writeKeyGroup(keyGroup(), keyGroupPrefixBytes, keyOut);
            SavepointSerializationUtils.writeKey(
                    currentEntry.getKey(), castToType(keySerializer), keyOut, isAmbigousKeyPossible);
            SavepointSerializationUtils.writeNameSpace(
                    currentEntry.getNamespace(),
                    castToType(currentStateSnapshot.getPreviousNamespaceSerializer()),
                    keyOut,
                    isAmbigousKeyPossible);
            TypeSerializer<?> previousStateSerializer =
                    currentStateSnapshot.getPreviousStateSerializer();
            switch (currentStateSnapshot.getStateType()) {
                case AGGREGATING:
                case REDUCING:
                case FOLDING:
                case VALUE:
                    castToType(previousStateSerializer)
                            .serialize(currentEntry.getState(), valueOut);
                    break;
                case LIST:
                    serializeValueList(
                            (List<Object>) currentEntry.getState(),
                            castToType(
                                    ((ListSerializer<?>) previousStateSerializer)
                                            .getElementSerializer()),
                            valueOut);
                    break;
                case MAP:
                    throw new UnsupportedOperationException();
                default:
                    throw new IllegalStateException("");
            }
        }

        @SuppressWarnings("unchecked")
        @Nonnull
        private <T> TypeSerializer<T> castToType(@Nonnull TypeSerializer<?> serializer) {
            return (TypeSerializer<T>) serializer;
        }
    }

    private class QueueIterator implements SingleStateIterator {
        public <T> QueueIterator(RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo) {
            
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public void writeOutNext() throws IOException {

        }
    }

    static <T> void serializeValueList(
            List<T> valueList,
            TypeSerializer<T> elementSerializer,
            DataOutputSerializer dataOutputView)
            throws IOException {

        boolean first = true;

        for (T value : valueList) {
            Preconditions.checkNotNull(value, "You cannot add null to a value list.");

            if (first) {
                first = false;
            } else {
                dataOutputView.write(';');
            }
            elementSerializer.serialize(value, dataOutputView);
        }
    }

    @Override
    public byte[] key() {
        return keyOut.getCopyOfBuffer();
    }

    @Override
    public byte[] value() {
        return valueOut.getCopyOfBuffer();
    }

    @Override
    public void close() {}
}
