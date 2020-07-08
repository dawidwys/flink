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

package org.apache.flink.runtime.operators.sort.v2;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.InMemorySorter;

import java.util.List;

/**
 * Class representing buffers that circulate between the reading, sorting and spilling thread.
 */
final class CircularElement<E> {

	final int id;
	final InMemorySorter<E> buffer;
	final List<MemorySegment> memory;

	public CircularElement() {
		this.id = -1;
		this.buffer = null;
		this.memory = null;
	}

	public CircularElement(int id, InMemorySorter<E> buffer, List<MemorySegment> memory) {
		this.id = id;
		this.buffer = buffer;
		this.memory = memory;
	}

	/**
	 * The element that is passed as marker for the end of data.
	 */
	static final CircularElement<Object> EOF_MARKER = new CircularElement<>();

	/**
	 * The element that is passed as marker for signal beginning of spilling.
	 */
	static final CircularElement<Object> SPILLING_MARKER = new CircularElement<>();

	/**
	 * Gets the element that is passed as marker for the end of data.
	 *
	 * @return The element that is passed as marker for the end of data.
	 */
	static <T> CircularElement<T> endMarker() {
		@SuppressWarnings("unchecked")
		CircularElement<T> c = (CircularElement<T>) EOF_MARKER;
		return c;
	}

	/**
	 * Gets the element that is passed as marker for signal beginning of spilling.
	 *
	 * @return The element that is passed as marker for signal beginning of spilling.
	 */
	static <T> CircularElement<T> spillingMarker() {
		@SuppressWarnings("unchecked")
		CircularElement<T> c = (CircularElement<T>) SPILLING_MARKER;
		return c;
	}
}
