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

import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimerHeapInternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.function.BiConsumerWithException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of a {@link InternalTimerService} that manages timers with a single active key at a time.
 * Can be used in a BATCH execution mode.
 */
public class SingleKeyInternalTimeService<K, N> implements InternalTimerService<N> {

	private final ProcessingTimeService processingTimeService;

	/**
	 * Processing time timers that are currently in-flight.
	 */
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;

	/**
	 * Event time timers that are currently in-flight.
	 */
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;

	/**
	 * The local event time, as denoted by the last received
	 * {@link org.apache.flink.streaming.api.watermark.Watermark Watermark}.
	 */
	private long currentWatermark = Long.MIN_VALUE;

	private final Triggerable<K, N> triggerTarget;

	private K currentKey;

	SingleKeyInternalTimeService(
			ProcessingTimeService processingTimeService,
			Triggerable<K, N> triggerTarget) {

		this.processingTimeService = checkNotNull(processingTimeService);
		this.triggerTarget = checkNotNull(triggerTarget);

		this.processingTimeTimersQueue = new SingleKeyKeyGroupedInternalPriorityQueue<>(
			PriorityComparator.forPriorityComparableObjects(),
			128
		);
		this.eventTimeTimersQueue = new SingleKeyKeyGroupedInternalPriorityQueue<>(
			PriorityComparator.forPriorityComparableObjects(),
			128
		);
	}

	@Override
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	public void registerProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
		processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, currentKey, namespace));
	}

	@Override
	public void registerEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, currentKey, namespace));
	}

	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		processingTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, currentKey, namespace));
	}

	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, currentKey, namespace));
	}

	@Override
	public void forEachEventTimeTimer(BiConsumerWithException<N, Long, Exception> consumer) throws Exception {
		foreachTimer(consumer, eventTimeTimersQueue);
	}

	@Override
	public void forEachProcessingTimeTimer(BiConsumerWithException<N, Long, Exception> consumer) throws Exception {
		foreachTimer(consumer, processingTimeTimersQueue);
	}

	private void foreachTimer(BiConsumerWithException<N, Long, Exception> consumer, KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> queue) throws Exception {
		try (final CloseableIterator<TimerHeapInternalTimer<K, N>> iterator = queue.iterator()) {
			while (iterator.hasNext()) {
				final TimerHeapInternalTimer<K, N> timer = iterator.next();
				consumer.accept(timer.getNamespace(), timer.getTimestamp());
			}
		}
	}

	public void setCurrentKey(K currentKey) throws Exception {
		currentWatermark = Long.MAX_VALUE;
		InternalTimer<K, N> timer;
		while ((timer = eventTimeTimersQueue.peek()) != null) {
			eventTimeTimersQueue.poll();
			triggerTarget.onEventTime(timer);
		}
		while ((timer = processingTimeTimersQueue.peek()) != null) {
			processingTimeTimersQueue.poll();
			triggerTarget.onProcessingTime(timer);
		}
		currentWatermark = Long.MIN_VALUE;
		this.currentKey = currentKey;
	}
}
