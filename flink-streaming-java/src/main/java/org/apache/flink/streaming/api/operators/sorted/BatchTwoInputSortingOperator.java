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
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * An operator for sorting elements based on their key and timestamp in a BATCH style execution.
 */
public class BatchTwoInputSortingOperator<K, IN1, IN2>
	extends AbstractStreamOperator<IN1>
	implements TwoInputStreamOperator<IN1, IN2, IN1>, BoundedMultiInput {

	private List<PushSorter<StreamElement>> sorters;
	private boolean[] finished;
	private List<KeySelector<Object, K>> keySelectors;

	private final TypeComparator<K> keyComparator;
	private final OutputTag<?>[] outputTags;

	public BatchTwoInputSortingOperator(
			TypeComparator<K> keyComparator,
			OutputTag<?>[] outputTags) {
		this.outputTags = outputTags;
		this.chainingStrategy = ChainingStrategy.HEAD;
		this.keyComparator = keyComparator;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setup(
			StreamTask<?, ?> containingTask,
			StreamConfig config,
			Output<StreamRecord<IN1>> output) {
		super.setup(containingTask, config, output);

		ClassLoader userCodeClassloader = getUserCodeClassloader();
		int numberOfInputs = config.getNumberOfInputs();

		if (outputTags.length != numberOfInputs - 1) {
			throw new IllegalStateException(
				"Provided " + outputTags.length + " output tags. Expected: " + (numberOfInputs - 1));
		}

		finished = new boolean[numberOfInputs];
		sorters = new ArrayList<>(numberOfInputs);
		keySelectors = new ArrayList<>(numberOfInputs);
		for (int i = 0; i < numberOfInputs; i++) {
			finished[i] = false;
			KeySelector<Object, K> keySelector = (KeySelector<Object, K>) config.getStatePartitioner(i, userCodeClassloader);
			keySelectors.add(keySelector);
			sorters.add(createSorter(i, numberOfInputs, containingTask, config, userCodeClassloader, keySelector));
		}
	}

	private PushSorter<StreamElement> createSorter(
			int inputIdx,
			int numberOfInputs,
			StreamTask<?, ?> containingTask,
			StreamConfig config,
			ClassLoader userCodeClassloader,
			KeySelector<Object, K> keySelector) {
		try {
			TypeSerializer<Object> typeSerializer = config.getTypeSerializerIn(inputIdx, userCodeClassloader);
			TypeSerializer<K> keySerializer = config.getStateKeySerializer(userCodeClassloader);

			KeyAndValueSerializer<?, K> streamElementSerializer = new KeyAndValueSerializer<>(
				typeSerializer,
				keySerializer,
				keySelector);
			StreamElementComparator<?, K> elementComparator = new StreamElementComparator<>(
				keySelector,
				keyComparator,
				keySerializer);
			Environment environment = containingTask.getEnvironment();
			return ExternalSorter.newBuilder(
				environment.getMemoryManager(),
				containingTask,
				streamElementSerializer,
				elementComparator)
				.enableSpilling(environment.getIOManager())
				.memoryFraction(1.0 / (float) numberOfInputs)
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

	@SuppressWarnings("unchecked")
	private void flush() throws Exception {
		if (!allFinished()) {
			return;
		}

		new MergeSortedEmitter(sorters.stream().map(sorter -> {
			try {
				sorter.finishReading();
				return sorter.getIterator();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}).toArray(MutableObjectIterator[]::new))
			.emit();
		output.emitWatermark(Watermark.MAX_WATERMARK);
	}

	private boolean allFinished() {
		for (boolean b : finished) {
			if (!b) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void endInput(int inputId) throws Exception {
		finished[inputId - 1] = true;
		if (allFinished()) {
			flush();
		}
	}

	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		this.sorters.get(0).writeRecord(element);
	}

	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		this.sorters.get(1).writeRecord(element);
	}

	private final class MergeSortedEmitter {
		private final PriorityQueue<HeadElement> queue;
		private final MutableObjectIterator<StreamElement>[] iterators;

		private MergeSortedEmitter(MutableObjectIterator<StreamElement>[] iterators) {
			this.iterators = iterators;
			queue = new PriorityQueue<>(iterators.length, new Comparator<HeadElement>() {
				@Override
				public int compare(
					HeadElement o1,
					HeadElement o2) {
					try {
						StreamRecord<Object> record1 = o1.streamElement.asRecord();
						StreamRecord<Object> record2 = o2.streamElement.asRecord();
						K key1 = keySelectors.get(o1.inputIndex).getKey(record1.getValue());
						K key2 = keySelectors.get(o2.inputIndex).getKey(record2.getValue());

						int compareKey = keyComparator.compare(key1, key2);

						if (compareKey == 0) {
							return Long.compare(record1.getTimestamp(), record2.getTimestamp());
						} else {
							return compareKey;
						}
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			});
		}

		public void emit() throws Exception {
			for (int i = 0; i < iterators.length; i++) {
				StreamElement next = iterators[i].next();
				if (next != null) {
					queue.add(new HeadElement(next, i));
				}
			}

			while (!queue.isEmpty()) {
				HeadElement element = queue.poll();
				int inputIndex = element.inputIndex;
				if (inputIndex == 0) {
					output.collect(element.streamElement.asRecord());
				} else {
					output.collect(outputTags[inputIndex - 1], element.streamElement.asRecord());
				}
				MutableObjectIterator<StreamElement> iterator = iterators[inputIndex];
				StreamElement next = iterator.next();
				if (next != null) {
					queue.add(new HeadElement(next, inputIndex));
				}
			}
		}
	}

	private static final class HeadElement {
		private final StreamElement streamElement;
		private final int inputIndex;

		private HeadElement(StreamElement streamElement, int inputIndex) {
			this.streamElement = streamElement;
			this.inputIndex = inputIndex;
		}
	}

}
