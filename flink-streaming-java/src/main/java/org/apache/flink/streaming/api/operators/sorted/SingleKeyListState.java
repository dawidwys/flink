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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link ListState} which keeps value for a single key at a time.
 */
public class SingleKeyListState<K, N, T>
		extends AbstractSingleKeyState<K, N, List<T>>
		implements InternalListState<K, N, T> {

	protected SingleKeyListState(
			List<T> defaultValue,
			TypeSerializer<List<T>> stateTypeSerializer) {
		super(defaultValue, stateTypeSerializer);
	}

	@Override
	public void update(List<T> values) throws Exception {
		if (values != null) {
			setCurrentNamespaceValue(new ArrayList<>(values));
		} else {
			setCurrentNamespaceValue(null);
		}
	}

	@Override
	public void addAll(List<T> values) throws Exception {
		initIfNull();
		getCurrentNamespaceValue().addAll(values);
	}

	@Override
	public void add(T value) throws Exception {
		initIfNull();
		getCurrentNamespaceValue().add(value);
	}

	private void initIfNull() {
		if (getCurrentNamespaceValue() == null) {
			setCurrentNamespaceValue(new ArrayList<>());
		}
	}

	@Override
	public Iterable<T> get() throws Exception {
		return getCurrentNamespaceValue();
	}

	@SuppressWarnings("unchecked")
	static <T, SV, S extends State, IS extends S> IS create(StateDescriptor<S, SV> stateDesc) {
		return (IS) new SingleKeyListState<>(
			(List<T>) stateDesc.getDefaultValue(),
			(TypeSerializer<List<T>>) stateDesc.getSerializer());
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {

	}

	@Override
	public List<T> getInternal() throws Exception {
		return getCurrentNamespaceValue();
	}

	@Override
	public void updateInternal(List<T> valueToStore) throws Exception {
		setCurrentNamespaceValue(valueToStore);
	}
}
