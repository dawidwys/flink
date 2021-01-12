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

package org.apache.flink.runtime.state.restore;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;

public class SnapshotUtil {

    public static final int FIRST_BIT_IN_BYTE_MASK = 0x80;

    public static final int END_OF_KEY_GROUP_MARK = 0xFFFF;

    public static void setMetaDataFollowsFlagInKey(byte[] key) {
        key[0] |= FIRST_BIT_IN_BYTE_MASK;
    }

    public static void clearMetaDataFollowsFlag(byte[] key) {
        key[0] &= (~FIRST_BIT_IN_BYTE_MASK);
    }

    public static boolean hasMetaDataFollowsFlag(byte[] key) {
        return 0 != (key[0] & FIRST_BIT_IN_BYTE_MASK);
    }

    public static int readKeyGroup(int keyGroupPrefixBytes, DataInputView inputView) throws IOException {
        int keyGroup = 0;
        for (int i = 0; i < keyGroupPrefixBytes; ++i) {
            keyGroup <<= 8;
            keyGroup |= (inputView.readByte() & 0xFF);
        }
        return keyGroup;
    }

    public static <K> K readKey(
            TypeSerializer<K> keySerializer,
            DataInputDeserializer inputView,
            boolean ambiguousKeyPossible)
            throws IOException {
        int beforeRead = inputView.getPosition();
        K key = keySerializer.deserialize(inputView);
        if (ambiguousKeyPossible) {
            int length = inputView.getPosition() - beforeRead;
            readVariableIntBytes(inputView, length);
        }
        return key;
    }

    public static <N> N readNamespace(
            TypeSerializer<N> namespaceSerializer,
            DataInputDeserializer inputView,
            boolean ambiguousKeyPossible)
            throws IOException {
        int beforeRead = inputView.getPosition();
        N namespace = namespaceSerializer.deserialize(inputView);
        if (ambiguousKeyPossible) {
            int length = inputView.getPosition() - beforeRead;
            readVariableIntBytes(inputView, length);
        }
        return namespace;
    }

    private static void readVariableIntBytes(DataInputView inputView, int value) throws IOException {
        do {
            inputView.readByte();
            value >>>= 8;
        } while (value != 0);
    }

    public static int computeRequiredBytesInKeyGroupPrefix(int totalKeyGroupsInJob) {
        return totalKeyGroupsInJob > (Byte.MAX_VALUE + 1) ? 2 : 1;
    }

    private SnapshotUtil() {
        throw new AssertionError();
    }
}
