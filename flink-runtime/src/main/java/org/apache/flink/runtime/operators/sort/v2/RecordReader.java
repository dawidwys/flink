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

import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;
import org.apache.flink.runtime.operators.sort.v2.StageRunner.SortStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RecordReader<E> {
	/** Logging. */
	private static final Logger LOG = LoggerFactory.getLogger(RecordReader.class);
	private final LargeRecordHandler<E> largeRecords;
	/** The object into which the thread reads the data from the input. */
	private final StageRunner.StageMessageDispatcher<E> dispatcher;
	private long bytesUntilSpilling;
	private CircularElement<E> currentBuffer;

	/**
	 * Creates a new reading thread.
	 *
	 * @param dispatcher The queues used to pass buffers between the threads.
	 */
	public RecordReader(
			StageRunner.StageMessageDispatcher<E> dispatcher,
			LargeRecordHandler<E> largeRecordsHandler,
			long startSpillingBytes) {

		// members
		this.bytesUntilSpilling = startSpillingBytes;
		this.largeRecords = largeRecordsHandler;
		this.dispatcher = dispatcher;

		if (bytesUntilSpilling < 1) {
			this.dispatcher.send(SortStage.SORT, CircularElement.spillingMarker());
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

	public void finishReading() {

		if (currentBuffer != null && !currentBuffer.buffer.isEmpty()) {
			this.dispatcher.send(SortStage.SORT, currentBuffer);
		}

		// add the sentinel to notify the receivers that the work is done
		// send the EOF marker
		final CircularElement<E> EOF_MARKER = CircularElement.endMarker();
		this.dispatcher.send(SortStage.SORT, EOF_MARKER);
		LOG.debug("Reading thread done.");
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
