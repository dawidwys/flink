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

package org.apache.flink.streaming.api.operators.sorted;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalKvState;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A common class for all internal states in single key state backend.
 */
abstract class AbstractSingleKeyState<K, N, V> implements InternalKvState<K, N, V> {

	private final V defaultValue;
	private final TypeSerializer<V> stateTypeSerializer;

	private final Map<N, V> valuesForNamespaces = new HashMap<>();
	private N currentNamespace;
	private V currentNamespaceValue;

	protected AbstractSingleKeyState(
			V defaultValue,
			TypeSerializer<V> stateTypeSerializer) {
		this.defaultValue = defaultValue;
		this.stateTypeSerializer = stateTypeSerializer;
	}

	V getOrDefault() {
		if (currentNamespaceValue == null && defaultValue != null) {
			return stateTypeSerializer.copy(defaultValue);
		}
		return currentNamespaceValue;
	}

	public V getCurrentNamespaceValue() {
		return currentNamespaceValue;
	}

	public void setCurrentNamespaceValue(V currentNamespaceValue) {
		this.currentNamespaceValue = currentNamespaceValue;
	}

	@Override
	public TypeSerializer<V> getValueSerializer() {
		return stateTypeSerializer;
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		throw new UnsupportedOperationException();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		if (Objects.equals(currentNamespace, namespace)) {
			return;
		}

		if (currentNamespace != null) {
			if (currentNamespaceValue == null) {
				valuesForNamespaces.remove(currentNamespace);
			} else {
				valuesForNamespaces.put(currentNamespace, currentNamespaceValue);
			}
		}
		currentNamespaceValue = valuesForNamespaces.get(namespace);
		currentNamespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(
			byte[] serializedKeyAndNamespace,
			TypeSerializer<K> safeKeySerializer,
			TypeSerializer<N> safeNamespaceSerializer,
			TypeSerializer<V> safeValueSerializer) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		this.currentNamespaceValue = null;
		this.valuesForNamespaces.remove(currentNamespace);
	}

	void clearAllNamespaces() {
		currentNamespaceValue = null;
		currentNamespace = null;
		valuesForNamespaces.clear();
	}
}
