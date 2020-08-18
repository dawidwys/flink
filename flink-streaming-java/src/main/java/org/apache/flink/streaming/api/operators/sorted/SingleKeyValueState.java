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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;

import java.io.IOException;

/**
 * A {@link ValueState} which keeps value for a single key at a time.
 */
public class SingleKeyValueState<K, N, T>
		extends AbstractSingleKeyState<K, N, T>
		implements InternalValueState<K, N, T> {

	SingleKeyValueState(
			T defaultValue,
			TypeSerializer<T> stateTypeSerializer) {
		super(defaultValue, stateTypeSerializer);
	}

	@Override
	public T value() throws IOException {
		return getOrDefault();
	}

	@Override
	public void update(T value) throws IOException {
		setCurrentNamespaceValue(value);
	}

	@SuppressWarnings("unchecked")
	static <T, SV, S extends State, IS extends S> IS create(StateDescriptor<S, SV> stateDesc) {
		return (IS) new SingleKeyValueState<>(stateDesc.getDefaultValue(), stateDesc.getSerializer());
	}
}
