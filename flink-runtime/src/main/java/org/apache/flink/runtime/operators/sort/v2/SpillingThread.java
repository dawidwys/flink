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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelReaderInputViewIterator;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExceptionHandler;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;
import org.apache.flink.runtime.operators.sort.MergeIterator;
import org.apache.flink.runtime.util.EmptyMutableObjectIterator;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.runtime.operators.sort.v2.CircularElement.EOF_MARKER;
import static org.apache.flink.runtime.operators.sort.v2.CircularElement.SPILLING_MARKER;

/**
 * The thread that handles the spilling of intermediate results and sets up the merging. It also merges the
 * channels until sufficiently few channels remain to perform the final streamed merge.
 */
final class SpillingThread<E> extends ThreadBase<E> {

	/** Logging. */
	private static final Logger LOG = LoggerFactory.getLogger(SpillingThread.class);

	protected final MemoryManager memManager;			// memory manager to release memory

	protected final IOManager ioManager;				// I/O manager to create channels

	protected final TypeSerializer<E> serializer;		// The serializer for the data type

	protected final TypeComparator<E> comparator;		// The comparator that establishes the order relation.

	protected final List<MemorySegment> writeMemory;	// memory segments for writing

	protected final List<MemorySegment> mergeReadMemory;	// memory segments for sorting/reading

	protected final int maxFanIn;

	protected final int numWriteBuffersToCluster;

	protected final SpillChannelManager spillChannelManager;

	private final LargeRecordHandler<E> largeRecordHandler;

	private final boolean objectReuseEnabled;

	/**
	 * Creates the spilling thread.
	 * @param exceptionHandler The exception handler to call for all exceptions.
	 * @param dispatcher The queues used to pass buffers between the threads.
	 * @param memManager The memory manager used to allocate buffers for the readers and writers.
	 * @param ioManager The I/I manager used to instantiate readers and writers from.
	 * @param serializer
	 * @param comparator
	 * @param sortReadMemory
	 * @param writeMemory
	 * @param maxNumFileHandles
	 * @param spillingChannelManager
	 * @param largeRecordHandler
	 */
	public SpillingThread(
			ExceptionHandler<IOException> exceptionHandler,
			StageRunner.StageMessageDispatcher<E> dispatcher,
			MemoryManager memManager,
			IOManager ioManager,
			TypeSerializer<E> serializer,
			TypeComparator<E> comparator,
			List<MemorySegment> sortReadMemory,
			List<MemorySegment> writeMemory,
			int maxNumFileHandles,
			SpillChannelManager spillingChannelManager,
			LargeRecordHandler<E> largeRecordHandler,
			boolean objectReuseEnabled) {
		super(exceptionHandler, "SortMerger spilling thread", dispatcher);
		this.memManager = memManager;
		this.ioManager = ioManager;
		this.serializer = serializer;
		this.comparator = comparator;
		this.mergeReadMemory = sortReadMemory;
		this.writeMemory = writeMemory;
		this.maxFanIn = maxNumFileHandles;
		this.numWriteBuffersToCluster = writeMemory.size() >= 4 ? writeMemory.size() / 2 : 1;
		this.spillChannelManager = spillingChannelManager;
		this.largeRecordHandler = largeRecordHandler;
		this.objectReuseEnabled = objectReuseEnabled;
	}

	/**
	 * Entry point of the thread.
	 */
	public void go() throws IOException {

		final Queue<CircularElement<E>> cache = new ArrayDeque<>();
		CircularElement<E> element;
		boolean cacheOnly = false;

		// ------------------- In-Memory Cache ------------------------
		// fill cache
		while (isRunning()) {
			// take next element from queue
			element = this.dispatcher.take(StageRunner.SortStage.SPILL);

			if (element == SPILLING_MARKER) {
				break;
			}
			else if (element == EOF_MARKER) {
				cacheOnly = true;
				break;
			}
			cache.add(element);
		}

		// check whether the thread was canceled
		if (!isRunning()) {
			return;
		}

		MutableObjectIterator<E> largeRecords = null;

		// check if we can stay in memory with the large record handler
		if (cacheOnly && largeRecordHandler != null && largeRecordHandler.hasData()) {
			List<MemorySegment> memoryForLargeRecordSorting = new ArrayList<MemorySegment>();

			CircularElement<E> circElement;
			while ((circElement = this.dispatcher.poll(StageRunner.SortStage.READ)) != null) {
				circElement.buffer.dispose();
				memoryForLargeRecordSorting.addAll(circElement.memory);
			}

			if (memoryForLargeRecordSorting.isEmpty()) {
				cacheOnly = false;
				LOG.debug("Going to disk-based merge because of large records.");
			} else {
				LOG.debug("Sorting large records, to add them to in-memory merge.");
				largeRecords = largeRecordHandler.finishWriteAndSortKeys(memoryForLargeRecordSorting);
			}
		}

		// ------------------- In-Memory Merge ------------------------
		if (cacheOnly) {
			// operates on in-memory buffers only
			LOG.debug("Initiating in memory merge.");

			List<MutableObjectIterator<E>> iterators = new ArrayList<MutableObjectIterator<E>>(cache.size() + 1);

			// iterate buffers and collect a set of iterators
			for (CircularElement<E> cached : cache) {
				// note: the yielded iterator only operates on the buffer heap (and disregards the stack)
				iterators.add(cached.buffer.getIterator());
			}

			if (largeRecords != null) {
				iterators.add(largeRecords);
			}

			// release the remaining sort-buffers
			LOG.debug("Releasing unused sort-buffer memory.");
			disposeSortBuffers(true);

			// set lazy iterator
			this.dispatcher.sendResult(iterators.isEmpty() ? EmptyMutableObjectIterator.<E>get() :
				iterators.size() == 1 ? iterators.get(0) :
					new MergeIterator<>(iterators, this.comparator));
			return;
		}

		// ------------------- Spilling Phase ------------------------

		final FileIOChannel.Enumerator enumerator = this.ioManager.createChannelEnumerator();
		List<ChannelWithBlockCount> channelIDs = new ArrayList<ChannelWithBlockCount>();

		// loop as long as the thread is marked alive and we do not see the final element
		while (isRunning()) {
			element = cache.isEmpty() ? this.dispatcher.take(StageRunner.SortStage.SPILL) : cache.poll();

			// check if we are still running
			if (!isRunning()) {
				return;
			}
			// check if this is the end-of-work buffer
			if (element == EOF_MARKER) {
				break;
			}

			// open next channel
			FileIOChannel.ID channel = enumerator.next();
			spillChannelManager.registerChannelToBeRemovedAtShudown(channel);

			// create writer
			final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(channel);
			spillChannelManager.registerOpenChannelToBeRemovedAtShudown(writer);
			final ChannelWriterOutputView output = new ChannelWriterOutputView(writer, this.writeMemory,
																		this.memManager.getPageSize());

			// write sort-buffer to channel
			LOG.debug("Spilling buffer " + element.id + ".");
			element.buffer.writeToOutput(output, largeRecordHandler);
			LOG.debug("Spilled buffer " + element.id + ".");

			output.close();
			spillChannelManager.unregisterOpenChannelToBeRemovedAtShudown(writer);

			if (output.getBytesWritten() > 0) {
				channelIDs.add(new ChannelWithBlockCount(channel, output.getBlockCount()));
			}

			// pass empty sort-buffer to reading thread
			element.buffer.reset();
			this.dispatcher.send(StageRunner.SortStage.READ, element);
		}

		// done with the spilling
		LOG.debug("Spilling done.");
		LOG.debug("Releasing sort-buffer memory.");

		// clear the sort buffers, but do not return the memory to the manager, as we use it for merging
		disposeSortBuffers(false);


		// ------------------- Merging Phase ------------------------

		// make sure we have enough memory to merge and for large record handling
		List<MemorySegment> mergeReadMemory;

		if (largeRecordHandler != null && largeRecordHandler.hasData()) {

			List<MemorySegment> longRecMem;
			if (channelIDs.isEmpty()) {
				// only long records
				longRecMem = this.mergeReadMemory;
				mergeReadMemory = Collections.emptyList();
			}
			else {
				int maxMergedStreams = Math.min(this.maxFanIn, channelIDs.size());

				int pagesPerStream = Math.max(
						ExternalSorter.MIN_NUM_WRITE_BUFFERS,
						Math.min(ExternalSorter.MAX_NUM_WRITE_BUFFERS, this.mergeReadMemory.size() / 2 / maxMergedStreams));

				int totalMergeReadMemory = maxMergedStreams * pagesPerStream;

				// grab the merge memory
				mergeReadMemory = new ArrayList<>(totalMergeReadMemory);
				for (int i = 0; i < totalMergeReadMemory; i++) {
					mergeReadMemory.add(this.mergeReadMemory.get(i));
				}

				// the remainder of the memory goes to the long record sorter
				longRecMem = new ArrayList<>();
				for (int i = totalMergeReadMemory; i < this.mergeReadMemory.size(); i++) {
					longRecMem.add(this.mergeReadMemory.get(i));
				}
			}

			LOG.debug("Sorting keys for large records.");
			largeRecords = largeRecordHandler.finishWriteAndSortKeys(longRecMem);
		}
		else {
			mergeReadMemory = this.mergeReadMemory;
		}

		// merge channels until sufficient file handles are available
		while (isRunning() && channelIDs.size() > this.maxFanIn) {
			channelIDs = mergeChannelList(channelIDs, mergeReadMemory, this.writeMemory);
		}

		// from here on, we won't write again
		this.memManager.release(this.writeMemory);
		this.writeMemory.clear();

		// check if we have spilled some data at all
		if (channelIDs.isEmpty()) {
			if (largeRecords == null) {
				this.dispatcher.sendResult(EmptyMutableObjectIterator.get());
			} else {
				this.dispatcher.sendResult(largeRecords);
			}
		}
		else {
			LOG.debug("Beginning final merge.");

			// allocate the memory for the final merging step
			List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelIDs.size());

			// allocate the read memory and register it to be released
			getSegmentsForReaders(readBuffers, mergeReadMemory, channelIDs.size());

			// get the readers and register them to be released
			this.dispatcher.sendResult(getMergingIterator(channelIDs, readBuffers,
				new ArrayList<>(channelIDs.size()), largeRecords));
		}

		// done
		LOG.debug("Spilling and merging thread done.");
	}

	/**
	 * Releases the memory that is registered for in-memory sorted run generation.
	 */
	protected final void disposeSortBuffers(boolean releaseMemory) {
		CircularElement<E> element;
		while ((element = this.dispatcher.poll(StageRunner.SortStage.READ)) != null) {
			element.buffer.dispose();
			if (releaseMemory) {
				this.memManager.release(element.memory);
			}
		}
	}

	// ------------------------------------------------------------------------
	//                             Result Merging
	// ------------------------------------------------------------------------

	/**
	 * Returns an iterator that iterates over the merged result from all given channels.
	 *
	 * @param channelIDs The channels that are to be merged and returned.
	 * @param inputSegments The buffers to be used for reading. The list contains for each channel one
	 *                      list of input segments. The size of the <code>inputSegments</code> list must be equal to
	 *                      that of the <code>channelIDs</code> list.
	 * @return An iterator over the merged records of the input channels.
	 * @throws IOException Thrown, if the readers encounter an I/O problem.
	 */
	protected final MergeIterator<E> getMergingIterator(final List<ChannelWithBlockCount> channelIDs,
			final List<List<MemorySegment>> inputSegments, List<FileIOChannel> readerList, MutableObjectIterator<E> largeRecords)
		throws IOException
	{
		// create one iterator per channel id
		if (LOG.isDebugEnabled()) {
			LOG.debug("Performing merge of " + channelIDs.size() + " sorted streams.");
		}

		final List<MutableObjectIterator<E>> iterators = new ArrayList<MutableObjectIterator<E>>(channelIDs.size() + 1);

		for (int i = 0; i < channelIDs.size(); i++) {
			final ChannelWithBlockCount channel = channelIDs.get(i);
			final List<MemorySegment> segsForChannel = inputSegments.get(i);

			// create a reader. if there are multiple segments for the reader, issue multiple together per I/O request
			final BlockChannelReader<MemorySegment> reader = this.ioManager.createBlockChannelReader(channel.getChannel());

			readerList.add(reader);
			spillChannelManager.registerOpenChannelToBeRemovedAtShudown(reader);
			spillChannelManager.unregisterChannelToBeRemovedAtShudown(channel.getChannel());

			// wrap channel reader as a view, to get block spanning record deserialization
			final ChannelReaderInputView inView = new ChannelReaderInputView(reader, segsForChannel,
																		channel.getBlockCount(), false);
			iterators.add(new ChannelReaderInputViewIterator<E>(inView, null, this.serializer));
		}

		if (largeRecords != null) {
			iterators.add(largeRecords);
		}

		return new MergeIterator<E>(iterators, this.comparator);
	}

	/**
	 * Merges the given sorted runs to a smaller number of sorted runs.
	 *
	 * @param channelIDs The IDs of the sorted runs that need to be merged.
	 * @param allReadBuffers
	 * @param writeBuffers The buffers to be used by the writers.
	 * @return A list of the IDs of the merged channels.
	 * @throws IOException Thrown, if the readers or writers encountered an I/O problem.
	 */
	protected final List<ChannelWithBlockCount> mergeChannelList(final List<ChannelWithBlockCount> channelIDs,
				final List<MemorySegment> allReadBuffers, final List<MemorySegment> writeBuffers)
	throws IOException
	{
		// A channel list with length maxFanIn<sup>i</sup> can be merged to maxFanIn files in i-1 rounds where every merge
		// is a full merge with maxFanIn input channels. A partial round includes merges with fewer than maxFanIn
		// inputs. It is most efficient to perform the partial round first.
		final double scale = Math.ceil(Math.log(channelIDs.size()) / Math.log(this.maxFanIn)) - 1;

		final int numStart = channelIDs.size();
		final int numEnd = (int) Math.pow(this.maxFanIn, scale);

		final int numMerges = (int) Math.ceil((numStart - numEnd) / (double) (this.maxFanIn - 1));

		final int numNotMerged = numEnd - numMerges;
		final int numToMerge = numStart - numNotMerged;

		// unmerged channel IDs are copied directly to the result list
		final List<ChannelWithBlockCount> mergedChannelIDs = new ArrayList<ChannelWithBlockCount>(numEnd);
		mergedChannelIDs.addAll(channelIDs.subList(0, numNotMerged));

		final int channelsToMergePerStep = (int) Math.ceil(numToMerge / (double) numMerges);

		// allocate the memory for the merging step
		final List<List<MemorySegment>> readBuffers = new ArrayList<List<MemorySegment>>(channelsToMergePerStep);
		getSegmentsForReaders(readBuffers, allReadBuffers, channelsToMergePerStep);

		final List<ChannelWithBlockCount> channelsToMergeThisStep = new ArrayList<ChannelWithBlockCount>(channelsToMergePerStep);
		int channelNum = numNotMerged;
		while (isRunning() && channelNum < channelIDs.size()) {
			channelsToMergeThisStep.clear();

			for (int i = 0; i < channelsToMergePerStep && channelNum < channelIDs.size(); i++, channelNum++) {
				channelsToMergeThisStep.add(channelIDs.get(channelNum));
			}

			mergedChannelIDs.add(mergeChannels(channelsToMergeThisStep, readBuffers, writeBuffers));
		}

		return mergedChannelIDs;
	}

	/**
	 * Merges the sorted runs described by the given Channel IDs into a single sorted run. The merging process
	 * uses the given read and write buffers.
	 *
	 * @param channelIDs The IDs of the runs' channels.
	 * @param readBuffers The buffers for the readers that read the sorted runs.
	 * @param writeBuffers The buffers for the writer that writes the merged channel.
	 * @return The ID and number of blocks of the channel that describes the merged run.
	 */
	protected ChannelWithBlockCount mergeChannels(
			List<ChannelWithBlockCount> channelIDs,
			List<List<MemorySegment>> readBuffers,
			List<MemorySegment> writeBuffers) throws IOException {
		// the list with the readers, to be closed at shutdown
		final List<FileIOChannel> channelAccesses = new ArrayList<>(channelIDs.size());

		// the list with the target iterators
		final MergeIterator<E> mergeIterator = getMergingIterator(channelIDs, readBuffers, channelAccesses, null);

		// create a new channel writer
		final FileIOChannel.ID mergedChannelID = this.ioManager.createChannel();
		spillChannelManager.registerChannelToBeRemovedAtShudown(mergedChannelID);
		final BlockChannelWriter<MemorySegment> writer = this.ioManager.createBlockChannelWriter(mergedChannelID);
		spillChannelManager.registerOpenChannelToBeRemovedAtShudown(writer);
		final ChannelWriterOutputView output = new ChannelWriterOutputView(
			writer,
			writeBuffers,
			this.memManager.getPageSize());

		// read the merged stream and write the data back
		if (objectReuseEnabled) {
			final TypeSerializer<E> serializer = this.serializer;
			E rec = serializer.createInstance();
			while ((rec = mergeIterator.next(rec)) != null) {
				serializer.serialize(rec, output);
			}
		} else {
			E rec;
			while ((rec = mergeIterator.next()) != null) {
				serializer.serialize(rec, output);
			}
		}
		output.close();
		final int numBlocksWritten = output.getBlockCount();

		// register merged result to be removed at shutdown
		spillChannelManager.unregisterOpenChannelToBeRemovedAtShudown(writer);

		// remove the merged channel readers from the clear-at-shutdown list
		for (FileIOChannel access : channelAccesses) {
			access.closeAndDelete();
			spillChannelManager.unregisterOpenChannelToBeRemovedAtShudown(access);
		}

		return new ChannelWithBlockCount(mergedChannelID, numBlocksWritten);
	}

	/**
	 * Divides the given collection of memory buffers among {@code numChannels} sublists.
	 *
	 * @param target The list into which the lists with buffers for the channels are put.
	 * @param memory A list containing the memory buffers to be distributed. The buffers are not
	 *               removed from this list.
	 * @param numChannels The number of channels for which to allocate buffers. Must not be zero.
	 */
	protected final void getSegmentsForReaders(List<List<MemorySegment>> target,
		List<MemorySegment> memory, int numChannels)
	{
		// determine the memory to use per channel and the number of buffers
		final int numBuffers = memory.size();
		final int buffersPerChannelLowerBound = numBuffers / numChannels;
		final int numChannelsWithOneMore = numBuffers % numChannels;

		final Iterator<MemorySegment> segments = memory.iterator();

		// collect memory for the channels that get one segment more
		for (int i = 0; i < numChannelsWithOneMore; i++) {
			final ArrayList<MemorySegment> segs = new ArrayList<>(buffersPerChannelLowerBound + 1);
			target.add(segs);
			for (int k = buffersPerChannelLowerBound; k >= 0; k--) {
				segs.add(segments.next());
			}
		}

		// collect memory for the remaining channels
		for (int i = numChannelsWithOneMore; i < numChannels; i++) {
			final ArrayList<MemorySegment> segs = new ArrayList<>(buffersPerChannelLowerBound);
			target.add(segs);
			for (int k = buffersPerChannelLowerBound; k > 0; k--) {
				segs.add(segments.next());
			}
		}
	}
}
