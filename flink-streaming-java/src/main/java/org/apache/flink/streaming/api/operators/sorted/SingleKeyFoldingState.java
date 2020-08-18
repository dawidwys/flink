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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalFoldingState;

/**
 * A {@link FoldingState} which keeps value for a single key at a time.
 */
public class SingleKeyFoldingState<K, N, T, ACC>
	extends AbstractSingleKeyState<K, N, ACC>
	implements InternalFoldingState<K, N, T, ACC> {

	private final FoldFunction<T, ACC> foldFunction;

	public SingleKeyFoldingState(
			FoldFunction<T, ACC> foldFunction,
			TypeSerializer<ACC> accTypeSerializer,
			ACC defaultValue) {
		super(defaultValue, accTypeSerializer);
		this.foldFunction = foldFunction;
	}

	@Override
	public ACC get() throws Exception {
		return getOrDefault();
	}

	@Override
	public void add(T value) throws Exception {
		if (value == null) {
			clear();
			return;
		}
		setCurrentNamespaceValue(foldFunction.fold(get(), value));
	}

	@SuppressWarnings("unchecked")
	static <SV, S extends State, IS extends S> IS create(StateDescriptor<S, SV> stateDesc) {
		FoldFunction<SV, SV> foldFunction = ((FoldingStateDescriptor<SV, SV>) stateDesc).getFoldFunction();
		return (IS) new SingleKeyFoldingState<>(foldFunction, stateDesc.getSerializer(), stateDesc.getDefaultValue());
	}

	@Override
	public ACC getInternal() throws Exception {
		return getCurrentNamespaceValue();
	}

	@Override
	public void updateInternal(ACC valueToStore) throws Exception {
		setCurrentNamespaceValue(valueToStore);
	}
}
