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

	/** The fraction of the buffers that must be full before the spilling starts. */
	private final long startSpillingBytes;

	/** The object into which the thread reads the data from the input. */
	private final E readTarget;

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
			SortStageRunner.StageMessageDispatcher<E> dispatcher,
			LargeRecordHandler<E> largeRecordsHandler,
			E readTarget,
			long startSpillingBytes) {
		super(exceptionHandler, "SortMerger Reading Thread", dispatcher);

		// members
		this.reader = reader;
		this.readTarget = readTarget;
		this.startSpillingBytes = startSpillingBytes;
		this.largeRecords = largeRecordsHandler;
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
		long bytesUntilSpilling = this.startSpillingBytes;
		boolean done = false;

		// check if we should directly spill
		if (bytesUntilSpilling < 1) {
			bytesUntilSpilling = 0;

			// add the spilling marker
			this.dispatcher.send(SortStageRunner.SortStage.SORT, CircularElement.spillingMarker());
		}

		// now loop until all channels have no more input data
		while (!done && isRunning())
		{
			// grab the next buffer
			while (element == null) {
				element = this.dispatcher.take(SortStageRunner.SortStage.READ);
			}

			// get the new buffer and check it
			final InMemorySorter<E> buffer = element.buffer;
			if (!buffer.isEmpty()) {
				throw new IOException("New buffer is not empty.");
			}

				LOG.debug("Retrieved empty read buffer " + element.id + ".");

			// write the last leftover pair, if we have one
			if (leftoverRecord != null) {
				if (!buffer.write(leftoverRecord)) {

					// did not fit in a fresh buffer, must be large...
					if (this.largeRecords != null) {
							LOG.debug("Large record did not fit into a fresh sort buffer. Putting into large record store.");
						this.largeRecords.addRecord(leftoverRecord);
					}
					else {
						throw new IOException("The record exceeds the maximum size of a sort buffer (current maximum: "
								+ buffer.getCapacity() + " bytes).");
					}
					buffer.reset();
				}

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

					if (bytesUntilSpilling - buffer.getOccupancy() <= 0) {
						bytesUntilSpilling = 0;

						// send the spilling marker
						final CircularElement<E> SPILLING_MARKER = CircularElement.spillingMarker();
						this.dispatcher.send(SortStageRunner.SortStage.SORT, SPILLING_MARKER);

						// we drop out of this loop and continue with the loop that
						// does not have the check
						break;
					}
				}

				if (fullBuffer) {
					// buffer is full. it may be that the last element would have crossed the
					// spilling threshold, so check it
					if (bytesUntilSpilling > 0) {
						bytesUntilSpilling -= buffer.getCapacity();
						if (bytesUntilSpilling <= 0) {
							bytesUntilSpilling = 0;
							// send the spilling marker
							final CircularElement<E> SPILLING_MARKER = CircularElement.spillingMarker();
							this.dispatcher.send(SortStageRunner.SortStage.SORT, SPILLING_MARKER);
						}
					}

					// send the buffer
					if (LOG.isDebugEnabled()) {
						LOG.debug("Emitting full buffer from reader thread: " + element.id + ".");
					}
					this.dispatcher.send(SortStageRunner.SortStage.SORT, element);
					element = null;
					continue;
				}
			}
			else if (bytesUntilSpilling > 0) {
				// this block must not be entered, if the last loop dropped out because
				// the input is exhausted.
				bytesUntilSpilling -= buffer.getCapacity();
				if (bytesUntilSpilling <= 0) {
					bytesUntilSpilling = 0;
					// send the spilling marker
					final CircularElement<E> SPILLING_MARKER = CircularElement.spillingMarker();
					this.dispatcher.send(SortStageRunner.SortStage.SORT, SPILLING_MARKER);
				}
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

			// check whether the buffer is exhausted or the reader is
			if (leftoverRecord != null) {
				LOG.debug("Emitting full buffer from reader thread: " + element.id + ".");
			} else {
				done = true;
				LOG.debug("Emitting final buffer from reader thread: " + element.id + ".");
			}

			// we can use add to add the element because we have no capacity restriction
			if (!buffer.isEmpty()) {
				this.dispatcher.send(SortStageRunner.SortStage.SORT, element);
			}
			else {
				buffer.reset();
				this.dispatcher.send(SortStageRunner.SortStage.READ, element);
			}
			element = null;
		}

		// we read all there is to read, or we are no longer running
		if (!isRunning()) {
			return;
		}

		// add the sentinel to notify the receivers that the work is done
		// send the EOF marker
		final CircularElement<E> EOF_MARKER = CircularElement.endMarker();
		this.dispatcher.send(SortStageRunner.SortStage.SORT, EOF_MARKER);
		LOG.debug("Reading thread done.");
	}
}
