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
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;

import javax.annotation.Nonnull;

import java.util.Set;

public class SingleKeyKeyGroupedInternalPriorityQueue<T extends HeapPriorityQueueElement>
		extends HeapPriorityQueue<T>
		implements KeyGroupedInternalPriorityQueue<T> {

	/**
	 * Creates an empty {@link HeapPriorityQueue} with the requested initial capacity.
	 *
	 * @param elementPriorityComparator comparator for the priority of contained elements.
	 * @param minimumCapacity the minimum and initial capacity of this priority queue.
	 */
	SingleKeyKeyGroupedInternalPriorityQueue(
			@Nonnull PriorityComparator<T> elementPriorityComparator,
			int minimumCapacity) {
		super(elementPriorityComparator, minimumCapacity);
	}

	@Nonnull
	@Override
	public Set<T> getSubsetForKeyGroup(int keyGroupId) {
		throw new UnsupportedOperationException("Getting subset for key group is not supported in BATCH runtime mode.");
	}
}
