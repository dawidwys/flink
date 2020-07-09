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
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The thread that consumes the input data and puts it into a buffer that will be sorted.
 */
class ReadingThread<E> extends ThreadBase<E> {

	/** Logging. */
	private static final Logger LOG = LoggerFactory.getLogger(ReadingThread.class);

	/** The input channels to read from. */
	private final MutableObjectIterator<E> reader;

	private final LargeRecordHandler<E> largeRecords;

	/** The object into which the thread reads the data from the input. */
	private final E readTarget;
	private long bytesUntilSpilling;

	private CircularElement<E> currentBuffer;

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
		this.reader = reader;
		this.readTarget = readTarget;
		this.bytesUntilSpilling = startSpillingBytes;
		this.largeRecords = largeRecordsHandler;

		signalSpillingIfNecessary(0);
	}

	public void go() throws IOException {
		final MutableObjectIterator<E> reader = this.reader;

		E current = reader.next(readTarget);
		while (isRunning() && (current != null)) {
			writeRecord(current);
			current = reader.next(current);
		}

		if (!currentBuffer.buffer.isEmpty()) {
			this.dispatcher.send(SortStage.SORT, currentBuffer);
		}

		if (isRunning()) {
			finishReading();
		}
	}

	public void writeRecord(E record) throws IOException {

		if (currentBuffer == null) {
			this.currentBuffer = this.dispatcher.take(SortStage.READ);
		}

		InMemorySorter<E> sorter = currentBuffer.buffer;

		long occupancyPreWrite = sorter.getOccupancy();
		if (!sorter.write(record)) {
			boolean isLarge = sorter.isEmpty();
			if (sorter.isEmpty()) {
				// did not fit in a fresh buffer, must be large...
				writeLarge(record, sorter, occupancyPreWrite);
			}
			this.dispatcher.send(SortStage.SORT, currentBuffer);
			if (!isLarge) {
				this.currentBuffer = this.dispatcher.take(SortStage.READ);
				writeRecord(record);
			}
		} else {
			long recordSize = sorter.getOccupancy() - occupancyPreWrite;
			signalSpillingIfNecessary(recordSize);
		}
	}

	private void writeLarge(E record, InMemorySorter<E> sorter, long occupancyPreWrite) throws IOException {
		if (this.largeRecords != null) {
			LOG.debug("Large record did not fit into a fresh sort buffer. Putting into large record store.");
			this.largeRecords.addRecord(record);
			long remainingCapacity = sorter.getCapacity() - occupancyPreWrite;
			signalSpillingIfNecessary(remainingCapacity);
		} else {
			throw new IOException("The record exceeds the maximum size of a sort buffer (current maximum: "
				+ sorter.getCapacity() + " bytes).");
		}
	}

	private void finishReading() {
		// add the sentinel to notify the receivers that the work is done
		// send the EOF marker
		final CircularElement<E> EOF_MARKER = CircularElement.endMarker();
		this.dispatcher.send(SortStage.SORT, EOF_MARKER);
		LOG.debug("Reading thread done.");
	}

	private void signalSpillingIfNecessary(long writtenSize) {
		if (bytesUntilSpilling <= 0) {
			return;
		}

		bytesUntilSpilling -= writtenSize;
		if (bytesUntilSpilling < 1) {
			// add the spilling marker
			this.dispatcher.send(SortStage.SORT, CircularElement.spillingMarker());
			bytesUntilSpilling = 0;
		}
	}
}
