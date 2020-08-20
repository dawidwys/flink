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
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.internal.InternalKeyedStateBackend;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.WrappingRuntimeException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of a {@link InternalTimeServiceManager} that manages timers with a single active key at a time.
 * Can be used in a BATCH execution mode.
 */
public class SingleKeyInternalTimeServiceManager<K> implements InternalTimeServiceManager<K>,
	KeyedStateBackend.KeySelectionListener<K> {

	private final ProcessingTimeService processingTimeService;
	private final Map<String, SingleKeyInternalTimeService<K, ?>> timerServices = new HashMap<>();

	public SingleKeyInternalTimeServiceManager(ProcessingTimeService processingTimeService) {
		this.processingTimeService = processingTimeService;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <N> InternalTimerService<N> getInternalTimerService(
			String name,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerable) {
		SingleKeyInternalTimeService<K, N> timerService = (SingleKeyInternalTimeService<K, N>) timerServices.get(name);
		if (timerService == null) {
			timerService = new SingleKeyInternalTimeService<>(
				processingTimeService,
				triggerable
			);
			timerServices.put(name, timerService);
		}

		return timerService;
	}

	@Override
	public void advanceWatermark(Watermark watermark) throws Exception {
		if (watermark.getTimestamp() == Long.MAX_VALUE) {
			keySelected(null);
		}
	}

	@Override
	public void snapshotState(
			KeyedStateBackend<?> keyedStateBackend,
			StateSnapshotContext context,
			String operatorName) throws Exception {
		throw new UnsupportedOperationException("Checkpoints are not supported in BATCH execution");
	}

	@Override
	public int numProcessingTimeTimers() {
		return 0; //ignore
	}

	@Override
	public int numEventTimeTimers() {
		return 0; //ignore
	}

	public static <K> InternalTimeServiceManager<K> create(
			ClassLoader userClassloader,
			InternalKeyedStateBackend<K> keyedStatedBackend,
			KeyContext keyContext, //the operator
			ProcessingTimeService processingTimeService,
			Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates) throws IOException {
		if (!(keyedStatedBackend instanceof SingleKeyKeyedStateBackend)) {
			throw new IllegalStateException(
				"Single key internal time service can work only with SingleKeyKeyedStateBackend");
		}

		SingleKeyInternalTimeServiceManager<K> timeServiceManager = new SingleKeyInternalTimeServiceManager<>(
			processingTimeService
		);
		keyedStatedBackend.registerKeySelectionListener(timeServiceManager);
		return timeServiceManager;
	}

	@Override
	public void keySelected(K newKey) {
		try {
			for (SingleKeyInternalTimeService<K, ?> value : timerServices.values()) {
				value.setCurrentKey(newKey);
			}
		} catch (Exception e) {
			throw new WrappingRuntimeException(e);
		}
	}
}
