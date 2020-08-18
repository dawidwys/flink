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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;

import java.io.IOException;
import java.util.Collection;

/**
 * A {@link AggregatingState} which keeps value for a single key at a time.
 */
public class SingleKeyAggregatingState<K, N, IN, ACC, OUT>
		extends AbstractSingleKeyState<K, N, ACC>
		implements InternalAggregatingState<K, N, IN, ACC, OUT> {

	private final AggregateFunction<IN, ACC, OUT> aggFunction;

	public SingleKeyAggregatingState(
			ACC defaultValue,
			AggregateFunction<IN, ACC, OUT> aggregateFunction,
			TypeSerializer<ACC> stateSerializer) {
		super(defaultValue, stateSerializer);
		this.aggFunction = aggregateFunction;
	}

	@Override
	public OUT get() {
		ACC acc = getOrDefault();
		return acc != null ? aggFunction.getResult(acc) : null;
	}

	@Override
	public void add(IN value) throws IOException {
		if (value == null) {
			clear();
			return;
		}

		try {
			if (getCurrentNamespaceValue() == null) {
				setCurrentNamespaceValue(aggFunction.createAccumulator());
			}
			setCurrentNamespaceValue(aggFunction.add(value, getCurrentNamespaceValue()));
		} catch (Exception e) {
			throw new IOException("Exception while applying AggregateFunction in aggregating state", e);
		}
	}

	@SuppressWarnings("unchecked")
	static <T, SV, S extends State, IS extends S> IS create(StateDescriptor<S, SV> stateDesc) {
		return (IS) new SingleKeyAggregatingState<>(
			stateDesc.getDefaultValue(),
			((AggregatingStateDescriptor<T, SV, ?>) stateDesc).getAggregateFunction(),
			stateDesc.getSerializer());
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {

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
