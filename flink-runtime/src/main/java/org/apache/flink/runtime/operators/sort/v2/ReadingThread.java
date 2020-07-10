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

import org.apache.flink.runtime.operators.sort.ExceptionHandler;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * The thread that consumes the input data and puts it into a buffer that will be sorted.
 */
class ReadingThread<E> extends ThreadBase<E> {

	/** The input channels to read from. */
	private final MutableObjectIterator<E> reader;

	/** The object into which the thread reads the data from the input. */
	private final E readTarget;

	private final RecordReader<E> recordProducer;

	/**
	 * Creates a new reading thread.
	 *
	 * @param exceptionHandler The exception handler to call for all exceptions.
	 * @param reader The reader to pull the data from.
	 * @param dispatcher The queues used to pass buffers between the threads.
	 */
	public ReadingThread(
			ExceptionHandler<IOException> exceptionHandler,
			MutableObjectIterator<E> reader,
			StageRunner.StageMessageDispatcher<E> dispatcher,
			LargeRecordHandler<E> largeRecordsHandler,
			E readTarget,
			long startSpillingBytes) {
		super(exceptionHandler, "SortMerger Reading Thread", dispatcher);

		// members
		this.recordProducer = new RecordReader<>(dispatcher, largeRecordsHandler, startSpillingBytes);
		this.reader = reader;
		this.readTarget = readTarget;
	}

	public void go() throws IOException {
		final MutableObjectIterator<E> reader = this.reader;

		E current = reader.next(readTarget);
		while (isRunning() && (current != null)) {
			recordProducer.writeRecord(current);
			current = reader.next(current);
		}

		recordProducer.finishReading();
	}
}
