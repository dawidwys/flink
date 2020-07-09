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

		signalSpillingIfNecessary();
	}

	public void newGo() throws IOException {
		final MutableObjectIterator<E> reader = this.reader;

		E current = reader.next(readTarget);
		while (isRunning() && (current != null)) {
			writeRecord(current);
			current = reader.next(current);
		}

		if (isRunning()) {
			finishReading();
		}
	}

	public void writeRecord(E record) throws IOException {
		CircularElement<E> buffer = this.dispatcher.take(SortStage.READ);
		InMemorySorter<E> sorter = buffer.buffer;

		if (!sorter.write(record)) {
			if (sorter.isEmpty()) {
				// did not fit in a fresh buffer, must be large...
				if (this.largeRecords != null) {
					LOG.debug("Large record did not fit into a fresh sort buffer. Putting into large record store.");
					this.largeRecords.addRecord(record);
				} else {
					throw new IOException("The record exceeds the maximum size of a sort buffer (current maximum: "
						+ sorter.getCapacity() + " bytes).");
				}
			}
		}
	}

	/**
	 * The entry point for the thread. Gets a buffer for all threads and then loops as long as there is input
	 * available.
	 */
	public void go() throws IOException {

		final MutableObjectIterator<E> reader = this.reader;

		E current = this.readTarget;
		E leftoverRecord = null;

		CircularElement<E> element = null;
		boolean done = false;

		// now loop until all channels have no more input data
		while (!done && isRunning())
		{
			// grab the next buffer
			while (element == null) {
				element = this.dispatcher.take(StageRunner.SortStage.READ);
			}

			// get the new buffer and check it
			final InMemorySorter<E> buffer = element.buffer;
			if (!buffer.isEmpty()) {
				throw new IOException("New buffer is not empty.");
			}

			LOG.debug("Retrieved empty read buffer " + element.id + ".");

			// write the last leftover pair, if we have one
			if (leftoverRecord != null) {
				writeLeftOver(leftoverRecord, buffer);
				leftoverRecord = null;
			}

			// we have two distinct code paths, depending on whether the spilling
			// threshold will be crossed in the current buffer, or not.
			boolean available = true;
			if (bytesUntilSpilling > 0 && buffer.getCapacity() >= bytesUntilSpilling)
			{
				boolean fullBuffer = false;

				// spilling will be triggered while this buffer is filled
				// loop until the buffer is full or the reader is exhausted
				E newCurrent;
				while (isRunning() && (available = (newCurrent = reader.next(current)) != null))
				{
					current = newCurrent;
					if (!buffer.write(current)) {
						leftoverRecord = current;
						fullBuffer = true;
						break;
					}

					// successfully added record

					bytesUntilSpilling = bytesUntilSpilling - buffer.getOccupancy();
					if (signalSpillingIfNecessary()) {
						break;
					}
				}

				if (fullBuffer) {
					// buffer is full. it may be that the last element would have crossed the
					// spilling threshold, so check it
					bytesUntilSpilling -= buffer.getCapacity();
					signalSpillingIfNecessary();

					// send the buffer
					LOG.debug("Emitting full buffer from reader thread: " + element.id + ".");
					this.dispatcher.send(StageRunner.SortStage.SORT, element);
					element = null;
					continue;
				}
			} else if (bytesUntilSpilling > 0) {
				// this block must not be entered, if the last loop dropped out because
				// the input is exhausted.
				bytesUntilSpilling -= buffer.getCapacity();
				signalSpillingIfNecessary();
			}

			// no spilling will be triggered (any more) while this buffer is being processed
			// loop until the buffer is full or the reader is exhausted
			if (available) {
				E newCurrent;
				while (isRunning() && ((newCurrent = reader.next(current)) != null)) {
					current = newCurrent;
					if (!buffer.write(current)) {
						leftoverRecord = current;
						break;
					}
				}
			}
			done = wasEntireRecordWritten(leftoverRecord, element);
			finalizeBuffer(element);
			element = null;
		}

		// we read all there is to read, or we are no longer running
		if (!isRunning()) {
			return;
		}
		finishReading();
	}

	private boolean wasEntireRecordWritten(E leftoverRecord, CircularElement<E> element) {
		// check whether the buffer is exhausted or the reader is
		if (leftoverRecord != null) {
			LOG.debug("Emitting full buffer from reader thread: " + element.id + ".");
			return false;
		} else {
			LOG.debug("Emitting final buffer from reader thread: " + element.id + ".");
			return true;
		}
	}

	private void finalizeBuffer(CircularElement<E> element) {
		final InMemorySorter<E> buffer = element.buffer;
		// we can use add to add the element because we have no capacity restriction
		if (!buffer.isEmpty()) {
			this.dispatcher.send(SortStage.SORT, element);
		} else {
			buffer.reset();
			this.dispatcher.send(SortStage.READ, element);
		}
	}

	private void finishReading() {
		// add the sentinel to notify the receivers that the work is done
		// send the EOF marker
		final CircularElement<E> EOF_MARKER = CircularElement.endMarker();
		this.dispatcher.send(SortStage.SORT, EOF_MARKER);
		LOG.debug("Reading thread done.");
	}

	private boolean signalSpillingIfNecessary() {
		if (bytesUntilSpilling < 1) {
			// add the spilling marker
			this.dispatcher.send(SortStage.SORT, CircularElement.spillingMarker());
			bytesUntilSpilling = 0;
			return true;
		}
		return false;
	}

	private void writeLeftOver(E leftoverRecord, InMemorySorter<E> buffer) throws IOException {
		if (!buffer.write(leftoverRecord)) {

			// did not fit in a fresh buffer, must be large...
			if (this.largeRecords != null) {
				LOG.debug("Large record did not fit into a fresh sort buffer. Putting into large record store.");
				this.largeRecords.addRecord(leftoverRecord);
			} else {
				throw new IOException("The record exceeds the maximum size of a sort buffer (current maximum: "
					+ buffer.getCapacity() + " bytes).");
			}
			buffer.reset();
		}
	}
}
