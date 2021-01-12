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

package org.apache.flink.runtime.state.restore;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.flink.runtime.state.StateUtil.unexpectedStateHandleException;
import static org.apache.flink.runtime.state.restore.SnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.runtime.state.restore.SnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.runtime.state.restore.SnapshotUtil.hasMetaDataFollowsFlag;

public class SavepointRestoreOperation<K>
        implements RestoreOperation<List<SavepointRestoreResult>> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private final KeyGroupRange keyGroupRange;
    private final CloseableRegistry cancelStreamRegistry;
    private final ClassLoader userCodeClassLoader;
    private final Collection<KeyedStateHandle> restoreStateHandles;
    private final StateSerializerProvider<K> keySerializerProvider;

    private boolean isKeySerializerCompatibilityChecked;

    public SavepointRestoreOperation(
            KeyGroupRange keyGroupRange,
            CloseableRegistry cancelStreamRegistry,
            ClassLoader userCodeClassLoader,
            Collection<KeyedStateHandle> restoreStateHandles,
            StateSerializerProvider<K> keySerializerProvider) {
        this.keyGroupRange = keyGroupRange;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.userCodeClassLoader = userCodeClassLoader;
        this.restoreStateHandles = restoreStateHandles;
        this.keySerializerProvider = keySerializerProvider;
    }

    @Override
    public List<SavepointRestoreResult> restore() throws IOException, StateMigrationException {
        List<SavepointRestoreResult> savepointRestoreResults = new ArrayList<>();
        for (KeyedStateHandle keyedStateHandle : restoreStateHandles) {
            if (keyedStateHandle != null) {

                if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
                    throw unexpectedStateHandleException(
                            KeyGroupsStateHandle.class, keyedStateHandle.getClass());
                }
                KeyGroupsStateHandle groupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
                savepointRestoreResults.add(restoreKeyGroupsInStateHandle(groupsStateHandle));
            }
        }
        return savepointRestoreResults;
    }

    /**
     * Restore one key groups state handle.
     *
     * @return
     */
    private SavepointRestoreResult restoreKeyGroupsInStateHandle(KeyGroupsStateHandle keyedStateHandle)
            throws IOException, StateMigrationException {
        FSDataInputStream currentStateHandleInStream = null;
        try {
            LOG.info("Starting to restore from state handle: {}.", keyedStateHandle);
            currentStateHandleInStream = keyedStateHandle.openInputStream();
            cancelStreamRegistry.registerCloseable(currentStateHandleInStream);
            KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(new DataInputViewStreamWrapper(
                    currentStateHandleInStream));
            KeyGroupsIterator groupsIterator = new KeyGroupsIterator(
                    keyGroupRange,
                    keyedStateHandle,
                    currentStateHandleInStream,
                    serializationProxy.isUsingKeyGroupCompression()
                            ? SnappyStreamCompressionDecorator.INSTANCE
                            : UncompressedStreamCompressionDecorator.INSTANCE
            );
            LOG.info("Finished restoring from state handle: {}.", keyedStateHandle);

            return new SavepointRestoreResult(
                    serializationProxy.getStateMetaInfoSnapshots(),
                    groupsIterator
            );
        } finally {
            if (cancelStreamRegistry.unregisterCloseable(currentStateHandleInStream)) {
                IOUtils.closeQuietly(currentStateHandleInStream);
            }
        }
    }

    private KeyedBackendSerializationProxy<K> readMetaData(DataInputView dataInputView)
            throws IOException, StateMigrationException {
        // isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
        // deserialization of state happens lazily during runtime; we depend on the fact
        // that the new serializer for states could be compatible, and therefore the restore can
        // continue
        // without old serializers required to be present.
        KeyedBackendSerializationProxy<K> serializationProxy =
                new KeyedBackendSerializationProxy<>(userCodeClassLoader);
        serializationProxy.read(dataInputView);
        if (!isKeySerializerCompatibilityChecked) {
            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            TypeSerializer<K> currentSerializer = keySerializerProvider.currentSchemaSerializer();
            // check for key serializer compatibility; this also reconfigures the
            // key serializer to be compatible, if it is required and is possible
            TypeSerializerSchemaCompatibility<K> keySerializerSchemaCompat =
                    keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(
                            serializationProxy.getKeySerializerSnapshot());
            if (keySerializerSchemaCompat.isCompatibleAfterMigration()
                    || keySerializerSchemaCompat.isIncompatible()) {
                throw new StateMigrationException(
                        "The new key serializer ("
                                + currentSerializer
                                + ") must be compatible with the previous key serializer ("
                                + keySerializerProvider.previousSchemaSerializer()
                                + ").");
            }

            isKeySerializerCompatibilityChecked = true;
        }

        return serializationProxy;
    }

    private static class KeyGroupsIterator implements ExceptionIterator<KeyGroup> {
        private final KeyGroupRange keyGroupRange;
        private final Iterator<Tuple2<Integer, Long>> keyGroups;
        /** Current input stream we obtained from currentKeyGroupsStateHandle. */
        private final FSDataInputStream currentStateHandleInStream;
        /**
         * The compression decorator that was used for writing the state, as determined by the meta
         * data.
         */
        private final StreamCompressionDecorator keygroupStreamCompressionDecorator;

        private KeyGroupsIterator(
                KeyGroupRange keyGroupRange,
                KeyGroupsStateHandle currentKeyGroupsStateHandle,
                FSDataInputStream currentStateHandleInStream,
                StreamCompressionDecorator keygroupStreamCompressionDecorator) {
            this.keyGroupRange = keyGroupRange;
            this.keyGroups = currentKeyGroupsStateHandle.getGroupRangeOffsets().iterator();
            this.currentStateHandleInStream = currentStateHandleInStream;
            this.keygroupStreamCompressionDecorator = keygroupStreamCompressionDecorator;
        }

        public boolean hasNext() {
            return keyGroups.hasNext();
        }

        public KeyGroup next() throws IOException {
            Tuple2<Integer, Long> keyGroupOffset = keyGroups.next();
            int keyGroup = keyGroupOffset.f0;

            // Check that restored key groups all belong to the backend
            Preconditions.checkState(
                    keyGroupRange.contains(keyGroup),
                    "The key group must belong to the backend");

            long offset = keyGroupOffset.f1;
            if (0L != offset) {
                currentStateHandleInStream.seek(offset);
                try (
                        InputStream compressedKgIn =
                                keygroupStreamCompressionDecorator.decorateWithCompression(
                                        currentStateHandleInStream)) {
                    DataInputViewStreamWrapper compressedKgInputView =
                            new DataInputViewStreamWrapper(compressedKgIn);
                    // TODO this could be aware of keyGroupPrefixBytes and write only one byte
                    // if possible
                    return new KeyGroup(
                            keyGroup,
                            new KeyGroupEntriesIterator(compressedKgInputView)
                    );
                }
            } else {
                return new KeyGroup(
                        keyGroup,
                        new KeyGroupEntriesIterator()
                );
            }
        }
    }

    private static class KeyGroupEntriesIterator implements ExceptionIterator<KeyGroupEntry> {
        private final DataInputViewStreamWrapper kgInputView;
        private Integer currentKvStateId;

        private KeyGroupEntriesIterator(DataInputViewStreamWrapper kgInputView) throws IOException {
            this.kgInputView = kgInputView;
            this.currentKvStateId = END_OF_KEY_GROUP_MARK & kgInputView.readShort();
        }

        // creates an empty iterator
        private KeyGroupEntriesIterator() {
            this.kgInputView = null;
            this.currentKvStateId = null;
        }

        public boolean hasNext() {
            return currentKvStateId != null;
        }

        public KeyGroupEntry next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            byte[] key =
                    BytePrimitiveArraySerializer.INSTANCE.deserialize(kgInputView);
            byte[] value =
                    BytePrimitiveArraySerializer.INSTANCE.deserialize(kgInputView);
            final int entryStateId = currentKvStateId;
            if (hasMetaDataFollowsFlag(key)) {
                // clear the signal bit in the key to make it ready for insertion
                // again
                clearMetaDataFollowsFlag(key);
                // TODO this could be aware of keyGroupPrefixBytes and write only
                // one byte if possible
                currentKvStateId =
                        END_OF_KEY_GROUP_MARK & kgInputView.readShort();
                if (END_OF_KEY_GROUP_MARK == currentKvStateId) {
                    currentKvStateId = null;
                }
            }

            return new KeyGroupEntry(
                    entryStateId,
                    key,
                    value
            );
        }
    }
}
