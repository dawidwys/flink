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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.operators.sort.v2.ExternalSorter;
import org.apache.flink.runtime.operators.sort.v2.PushSorter;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.MutableObjectIterator;

/**
 * An operator for sorting elements based on their key and timestamp in a BATCH style execution.
 */
public class BatchOneInputSortingOperator<IN, OUT, K> extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

	protected transient PushSorter<StreamElement> sorter;
	private final TypeComparator<K> keyComparator;

	public BatchOneInputSortingOperator(
			TypeComparator<K> keyComparator) {
		this.chainingStrategy = ChainingStrategy.HEAD;
		this.keyComparator = keyComparator;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setup(
				StreamTask<?, ?> containingTask,
				StreamConfig config,
				Output<StreamRecord<OUT>> output) {
			super.setup(containingTask, config, output);
		try {
			ClassLoader userCodeClassloader = getUserCodeClassloader();
			TypeSerializer<IN> typeSerializer = config.getTypeSerializerIn1(userCodeClassloader);
			KeySelector<IN, K> keySelector = (KeySelector<IN, K>) config.getStatePartitioner(0, userCodeClassloader);
			TypeSerializer<K> keySerializer = config.getStateKeySerializer(userCodeClassloader);

			KeyAndValueSerializer<IN, K> streamElementSerializer = new KeyAndValueSerializer<>(typeSerializer, keySerializer, keySelector);
			StreamElementComparator<IN, K> elementComparator = new StreamElementComparator<>(
				keySelector,
				keyComparator,
				keySerializer);
			Environment environment = containingTask.getEnvironment();
			this.sorter = ExternalSorter.newBuilder(
				environment.getMemoryManager(),
				containingTask,
				streamElementSerializer,
				elementComparator)
				.enableSpilling(environment.getIOManager())
				.memoryFraction(1.0)
				.startSpillingFraction(0.8)
				.maxNumFileHandles(128)
				.build();
		} catch (MemoryAllocationException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
	}

	@Override
	public void close() throws Exception {
		super.close();
		sorter.close();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		this.sorter.writeRecord(element);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// ignore watermarks
	}

	@Override
	public void endInput() throws Exception {
		this.sorter.finishReading();
		StreamElement next;
		MutableObjectIterator<StreamElement> iterator = sorter.getIterator();
		while ((next = iterator.next()) != null) {
			output.collect(next.asRecord());
		}
	}
}
