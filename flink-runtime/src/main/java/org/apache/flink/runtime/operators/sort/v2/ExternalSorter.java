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
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.DefaultInMemorySorterFactory;
import org.apache.flink.runtime.operators.sort.ExceptionHandler;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.InMemorySorterFactory;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;
import org.apache.flink.runtime.operators.sort.Sorter;
import org.apache.flink.util.MutableObjectIterator;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The {@link ExternalSorter} is a full fledged sorter. It implements a multi-way merge sort. Internally,
 * the logic is factored into three threads (read, sort, spill) which communicate through a set of blocking queues,
 * forming a closed loop.  Memory is allocated using the {@link MemoryManager} interface. Thus the component will
 * not exceed the provided memory limits.
 */
public class ExternalSorter<E> implements Sorter<E> {
	
	// ------------------------------------------------------------------------
	//                              Constants
	// ------------------------------------------------------------------------

	/** Logging. */
	private static final Logger LOG = LoggerFactory.getLogger(ExternalSorter.class);
	
	/** Fix length records with a length below this threshold will be in-place sorted, if possible. */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;
	
	/** The minimal number of buffers to use by the writers. */
	protected static final int MIN_NUM_WRITE_BUFFERS = 2;
	
	/** The maximal number of buffers to use by the writers. */
	protected static final int MAX_NUM_WRITE_BUFFERS = 4;
	
	/** The minimum number of segments that are required for the sort to operate. */
	protected static final int MIN_NUM_SORT_MEM_SEGMENTS = 10;
	
	// ------------------------------------------------------------------------
	//                                  Threads
	// ------------------------------------------------------------------------

	/** The thread that reads the input channels into buffers and passes them on to the merger. */
	private final StageRunner readThread;

	/** The thread that merges the buffer handed from the reading thread. */
	private final StageRunner sortThread;

	/** The thread that handles spilling to secondary storage. */
	private final StageRunner spillThread;
	
	// ------------------------------------------------------------------------
	//                                   Memory
	// ------------------------------------------------------------------------

	/** The memory segments used first for sorting and later for reading/pre-fetching
	 * during the external merge. */
	protected final List<MemorySegment> sortReadMemory;
	
	/** The memory segments used to stage data to be written. */
	protected final List<MemorySegment> writeMemory;

	/** The memory manager through which memory is allocated and released. */
	protected final MemoryManager memoryManager;

	// ------------------------------------------------------------------------
	//                            Miscellaneous Fields
	// ------------------------------------------------------------------------
	
	/**
	 * The handler for large records, that do not go though the in-memory sorter as a whole, but
	 * directly go to disk.
	 */
	private final LargeRecordHandler<E> largeRecordHandler;
	
	/**
	 * Collection of all currently open channels, to be closed and deleted during cleanup.
	 */
	private final SpillChannelManager spillChannelManager;

	private final CircularQueues<E> queues;

	/**
	 * Flag indicating that the sorter was closed.
	 */
	protected volatile boolean closed;

	private final Collection<InMemorySorter<E>> inMemorySorters;

	private ExternalSorter(
			StageRunner readThread,
			StageRunner sortThread,
			StageRunner spillThread,
			List<MemorySegment> sortReadMemory,
			List<MemorySegment> writeMemory,
			MemoryManager memoryManager,
			LargeRecordHandler<E> largeRecordHandler,
			SpillChannelManager spillChannelManager,
			Collection<InMemorySorter<E>> inMemorySorters,
			CircularQueues<E> queues) {
		this.readThread = readThread;
		this.sortThread = sortThread;
		this.spillThread = spillThread;
		this.sortReadMemory = sortReadMemory;
		this.writeMemory = writeMemory;
		this.memoryManager = memoryManager;
		this.largeRecordHandler = largeRecordHandler;
		this.spillChannelManager = spillChannelManager;
		this.inMemorySorters = inMemorySorters;
		this.queues = queues;
		startThreads();
	}

	/**
	 * Starts all the threads that are used by this sort-merger.
	 */
	protected void startThreads() {
		if (this.readThread != null) {
			this.readThread.start();
		}
		if (this.sortThread != null) {
			this.sortThread.start();
		}
		if (this.spillThread != null) {
			this.spillThread.start();
		}
	}

	/**
	 * Shuts down all the threads initiated by this sort/merger. Also releases all previously allocated
	 * memory, if it has not yet been released by the threads, and closes and deletes all channels (removing
	 * the temporary files).
	 * <p>
	 * The threads are set to exit directly, but depending on their operation, it may take a while to actually happen.
	 * The sorting thread will for example not finish before the current batch is sorted. This method attempts to wait
	 * for the working thread to exit. If it is however interrupted, the method exits immediately and is not guaranteed
	 * how long the threads continue to exist and occupy resources afterwards.
	 *
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() {
		// check if the sorter has been closed before
		synchronized (this) {
			if (this.closed) {
				return;
			}
			
			// mark as closed
			this.closed = true;
		}
		
		// from here on, the code is in a try block, because even through errors might be thrown in this block,
		// we need to make sure that all the memory is released.
		try {

			// stop all the threads
			if (this.readThread != null) {
				try {
					this.readThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down reader thread: " + t.getMessage(), t);
				}
			}
			if (this.sortThread != null) {
				try {
					this.sortThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down sorter thread: " + t.getMessage(), t);
				}
			}
			if (this.spillThread != null) {
				try {
					this.spillThread.shutdown();
				} catch (Throwable t) {
					LOG.error("Error shutting down spilling thread: " + t.getMessage(), t);
				}
			}
		}
		finally {

			// Dispose all in memory sorter in order to clear memory references
			for (InMemorySorter<?> inMemorySorter : inMemorySorters) {
				inMemorySorter.dispose();
			}

			// RELEASE ALL MEMORY. If the threads and channels are still running, this should cause
			// exceptions, because their memory segments are freed
			try {
				if (!this.writeMemory.isEmpty()) {
					this.memoryManager.release(this.writeMemory);
				}
				this.writeMemory.clear();
			}
			catch (Throwable ignored) {}
			
			try {
				if (!this.sortReadMemory.isEmpty()) {
					this.memoryManager.release(this.sortReadMemory);
				}
				this.sortReadMemory.clear();
			}
			catch (Throwable ignored) {}

			// we have to loop this, because it may fail with a concurrent modification exception
			this.spillChannelManager.close();

			try {
				if (this.largeRecordHandler != null) {
					this.largeRecordHandler.close();
				}
			} catch (Throwable ignored) {}
		}
	}



	// ------------------------------------------------------------------------
	//                           Factory Methods
	// ------------------------------------------------------------------------

	@FunctionalInterface
	public interface ReadingStageFactory<E> {
		/**
		 * Creates the reading thread. The reading thread simply reads the data off the input and puts it
		 * into the buffer where it will be sorted.
		 * <p>
		 * The returned thread is not yet started.
		 *
		 * @param exceptionHandler The handler for exceptions in the thread.
		 * @param dispatcher The queues through which the thread communicates with the other threads.
		 * @param startSpillingBytes The number of bytes after which the reader thread will send the notification to
		 * start the spilling.
		 * @return The thread that reads data from an input, writes it into sort buffers and puts
		 * them into a queue.
		 */
		StageRunner getReadingThread(
			ExceptionHandler<IOException> exceptionHandler,
			StageRunner.StageMessageDispatcher<E> dispatcher,
			LargeRecordHandler<E> largeRecordHandler,
			long startSpillingBytes);
	}

	public boolean isClosed() {
		return closed;
	}

	// ------------------------------------------------------------------------
	//                           Result Iterator
	// ------------------------------------------------------------------------

	@Override
	public MutableObjectIterator<E> getIterator() throws InterruptedException {
		return queues.getIteratorFuture().exceptionally(
			exception -> {
				throw new RuntimeException(
					"Error obtaining the sorted input: " + exception.getMessage(),
					exception);
			}
		).join();
	}

	public static <E> Builder<E> newBuilder(
			MemoryManager memoryManager,
			AbstractInvokable parentTask,
			TypeSerializer<E> serializerFactory,
			TypeComparator<E> comparator) {
		Preconditions.checkNotNull(memoryManager);
		Preconditions.checkNotNull(serializerFactory);
		Preconditions.checkNotNull(comparator);
		Preconditions.checkNotNull(parentTask);
		return new Builder<>(
			memoryManager,
			parentTask,
			serializerFactory,
			comparator
		);
	}

	public static class Builder<T> {

		private final MemoryManager memoryManager;
		private final AbstractInvokable parentTask;
		private final TypeSerializer<T> serializer;
		private final TypeComparator<T> comparator;
		private final InMemorySorterFactory<T> inMemorySorterFactory;

		private int maxNumFileHandles = 2;
		private boolean objectReuseEnabled = false;
		private boolean handleLargeRecords = false;
		private double memoryFraction = 0.7;
		private int numSortBuffers = -1;
		private double startSpillingFraction = 0; //TODO

		private IOManager ioManager;
		private boolean noSpillingMemory = true;


		public Builder(
				MemoryManager memoryManager,
				AbstractInvokable parentTask,
				TypeSerializer<T> serializer,
				TypeComparator<T> comparator) {
			this.memoryManager = memoryManager;
			this.parentTask = parentTask;
			this.serializer = serializer;
			this.comparator = comparator;
			this.inMemorySorterFactory = new DefaultInMemorySorterFactory<>(
				serializer,
				comparator,
				THRESHOLD_FOR_IN_PLACE_SORTING);
		}

		public Builder<T> maxNumFileHandles(int maxNumFileHandles) {
			if (maxNumFileHandles < 2) {
				throw new IllegalArgumentException("Merger cannot work with less than two file handles.");
			}
			this.maxNumFileHandles = maxNumFileHandles;
			return this;
		}

		public Builder<T> objectReuse(boolean enabled) {
			this.objectReuseEnabled = enabled;
			return this;
		}

		public Builder<T> largeRecords(boolean enabled) {
			this.handleLargeRecords = enabled;
			return this;
		}

		public Builder<T> enableSpilling(IOManager ioManager) {
			this.noSpillingMemory = false;
			this.ioManager = ioManager;
			return this;
		}

		public Builder<T> memoryFraction(double fraction) {
			this.memoryFraction = fraction;
			return this;
		}

		public Builder<T> sortBuffers(int numSortBuffers) {
			this.numSortBuffers = numSortBuffers;
			return this;
		}

		public Builder<T> startSpillingFraction(double startSpillingFraction) {
			this.startSpillingFraction = startSpillingFraction;
			return this;
		}

		public ExternalSorter<T> build(MutableObjectIterator<T> input) throws MemoryAllocationException {
			return doBuild((exceptionHandler, dispatcher, largeRecordHandler, startSpillingBytes) ->
				new ReadingThread<>(
					exceptionHandler,
					input,
					dispatcher,
					largeRecordHandler,
					serializer.createInstance(),
					startSpillingBytes
				)
			);
		}

		private static final class PushFactory<E> implements ReadingStageFactory<E> {

			private RecordReader<E> recordReader;

			@Override
			public StageRunner getReadingThread(
					ExceptionHandler<IOException> exceptionHandler,
					StageRunner.StageMessageDispatcher<E> dispatcher,
					LargeRecordHandler<E> largeRecordHandler, long startSpillingBytes) {
				recordReader = new RecordReader<>(
					dispatcher,
					largeRecordHandler,
					startSpillingBytes
				);
				return new PushBasedRecordProducer<>(recordReader);
			}
		}

		public PushSorter<T> build() throws MemoryAllocationException {
			PushFactory<T> pushFactory = new PushFactory<>();
			ExternalSorter<T> tExternalSorter = doBuild(pushFactory);

			return new PushSorter<T>() {

				private final RecordReader<T> recordProducer = pushFactory.recordReader;

				@Override
				public void writeRecord(T record) throws IOException {
					recordProducer.writeRecord(record);
				}

				@Override
				public void finishReading() {
					recordProducer.finishReading();
				}

				@Override
				public MutableObjectIterator<T> getIterator() throws InterruptedException {
					return tExternalSorter.getIterator();
				}

				@Override
				public void close() throws IOException {
					tExternalSorter.close();
				}
			};
		}

		private ExternalSorter<T> doBuild(ReadingStageFactory<T> readingStageFactory) throws MemoryAllocationException {

			List<MemorySegment> memory = memoryManager.allocatePages(
				parentTask,
				memoryManager.computeNumberOfPages(memoryFraction));

			// adjust the memory quotas to the page size
			final int numPagesTotal = memory.size();

			if (numPagesTotal < MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS) {
				throw new IllegalArgumentException("Too little memory provided to sorter to perform task. " +
					"Required are at least " + (MIN_NUM_WRITE_BUFFERS + MIN_NUM_SORT_MEM_SEGMENTS) +
					" pages. Current page size is " + memoryManager.getPageSize() + " bytes.");
			}

			// determine how many buffers to use for writing
			final int numWriteBuffers;
			final int numLargeRecordBuffers;

			if (noSpillingMemory && !handleLargeRecords) {
				numWriteBuffers = 0;
				numLargeRecordBuffers = 0;
			} else {
				int numConsumers = (noSpillingMemory ? 0 : 1) + (handleLargeRecords ? 2 : 0);

				// determine how many buffers we have when we do a full mere with maximal fan-in
				final int minBuffersForMerging = maxNumFileHandles + numConsumers * MIN_NUM_WRITE_BUFFERS;

				if (minBuffersForMerging > numPagesTotal) {
					numWriteBuffers = noSpillingMemory ? 0 : MIN_NUM_WRITE_BUFFERS;
					numLargeRecordBuffers = handleLargeRecords ? 2*MIN_NUM_WRITE_BUFFERS : 0;

					maxNumFileHandles = numPagesTotal - numConsumers * MIN_NUM_WRITE_BUFFERS;
					if (LOG.isDebugEnabled()) {
						LOG.debug("Reducing maximal merge fan-in to " + maxNumFileHandles + " due to limited memory availability during merge");
					}
				}
				else {
					// we are free to choose. make sure that we do not eat up too much memory for writing
					final int fractionalAuxBuffers = numPagesTotal / (numConsumers * 100);

					if (fractionalAuxBuffers >= MAX_NUM_WRITE_BUFFERS) {
						numWriteBuffers = noSpillingMemory ? 0 : MAX_NUM_WRITE_BUFFERS;
						numLargeRecordBuffers = handleLargeRecords ? 2 * MAX_NUM_WRITE_BUFFERS : 0;
					} else {
						numWriteBuffers = noSpillingMemory ? 0 :
							Math.max(MIN_NUM_WRITE_BUFFERS, fractionalAuxBuffers);    // at least the lower bound

						numLargeRecordBuffers = handleLargeRecords ?
							Math.max(2 * MIN_NUM_WRITE_BUFFERS, fractionalAuxBuffers) // at least the lower bound
							: 0;
					}
				}
			}

			final int sortMemPages = numPagesTotal - numWriteBuffers - numLargeRecordBuffers;
			final long sortMemory = ((long) sortMemPages) * memoryManager.getPageSize();

			// decide how many sort buffers to use
			if (numSortBuffers < 1) {
				if (sortMemory > 100 * 1024 * 1024) {
					numSortBuffers = 2;
				}
				else {
					numSortBuffers = 1;
				}
			}
			final int numSegmentsPerSortBuffer = sortMemPages / numSortBuffers;

			LOG.debug(String.format("Instantiating sorter with %d pages of sorting memory (="
					+ "%d bytes total) divided over %d sort buffers (%d pages per buffer). Using %d"
					+ " buffers for writing sorted results and merging maximally %d streams at once. "
					+ "Using %d memory segments for large record spilling.",
				sortMemPages, sortMemory, numSortBuffers, numSegmentsPerSortBuffer, numWriteBuffers,
				maxNumFileHandles, numLargeRecordBuffers));

			List<MemorySegment> writeMemory = new ArrayList<>(numWriteBuffers);

			LargeRecordHandler<T> largeRecordHandler;
			// move some pages from the sort memory to the write memory
			if (numWriteBuffers > 0) {
				for (int i = 0; i < numWriteBuffers; i++) {
					writeMemory.add(memory.remove(memory.size() - 1));
				}
			}
			if (numLargeRecordBuffers > 0) {
				List<MemorySegment> mem = new ArrayList<>();
				for (int i = 0; i < numLargeRecordBuffers; i++) {
					mem.add(memory.remove(memory.size() - 1));
				}

				largeRecordHandler = new LargeRecordHandler<>(serializer, comparator.duplicate(),
					ioManager, memoryManager, mem, parentTask, maxNumFileHandles);
			}
			else {
				largeRecordHandler = null;
			}

			// circular queues pass buffers between the threads
			final CircularQueues<T> circularQueues = new CircularQueues<>();

			final List<InMemorySorter<T>> inMemorySorters = new ArrayList<>(numSortBuffers);

			// allocate the sort buffers and fill empty queue with them
			final Iterator<MemorySegment> segments = memory.iterator();
			for (int i = 0; i < numSortBuffers; i++)
			{
				// grab some memory
				final List<MemorySegment> sortSegments = new ArrayList<>(numSegmentsPerSortBuffer);
				for (int k = (i == numSortBuffers - 1 ? Integer.MAX_VALUE : numSegmentsPerSortBuffer); k > 0 && segments.hasNext(); k--) {
					sortSegments.add(segments.next());
				}

				final InMemorySorter<T> inMemorySorter = inMemorySorterFactory.create(sortSegments);
				inMemorySorters.add(inMemorySorter);

				// add to empty queue
				CircularElement<T> element = new CircularElement<>(i, inMemorySorter, sortSegments);
				circularQueues.empty.add(element);
			}

			// exception handling
			ExceptionHandler<IOException> exceptionHandler = exception -> {
				circularQueues.getIteratorFuture().completeExceptionally(exception);
				// forward exception
//				if (!closed) {
//					iteratorFuture.completeExceptionally(exception);
//					close();
//				}
			};

			SpillChannelManager spillChannelManager = new SpillChannelManager();
			// start the thread that reads the input channels
			StageRunner readingThread = readingStageFactory.getReadingThread(
				exceptionHandler,
				circularQueues,
				largeRecordHandler,
				((long) (startSpillingFraction * sortMemory)));

			// start the thread that sorts the buffers
			StageRunner sortingStage = new SortingThread<>(exceptionHandler, circularQueues);

			// start the thread that handles spilling to secondary storage
			StageRunner spillingStage = new SpillingThread<>(
				exceptionHandler,
				circularQueues,
				memoryManager,
				ioManager,
				serializer,
				comparator,
				memory,
				writeMemory,
				maxNumFileHandles,
				spillChannelManager,
				largeRecordHandler,
				objectReuseEnabled);

			return new ExternalSorter<>(
				readingThread,
				sortingStage,
				spillingStage,
				memory,
				writeMemory,
				memoryManager,
				largeRecordHandler,
				spillChannelManager,
				inMemorySorters,
				circularQueues
			);
		}
	}

	// ------------------------------------------------------------------------
	// Inter-Thread Communication
	// ------------------------------------------------------------------------

	/**
	 * Collection of queues that are used for the communication between the threads.
	 */
	private static final class CircularQueues<E> implements StageRunner.StageMessageDispatcher<E> {

		final BlockingQueue<CircularElement<E>> empty;

		final BlockingQueue<CircularElement<E>> sort;

		final BlockingQueue<CircularElement<E>> spill;

		/**
		 * The iterator to be returned by the sort-merger. This variable is null, while receiving and merging is still in
		 * progress and it will be set once we have &lt; merge factor sorted sub-streams that will then be streamed sorted.
		 */
		private final CompletableFuture<MutableObjectIterator<E>> iteratorFuture = new CompletableFuture<>();

		public CircularQueues() {
			this.empty = new LinkedBlockingQueue<>();
			this.sort = new LinkedBlockingQueue<>();
			this.spill = new LinkedBlockingQueue<>();
		}

		private BlockingQueue<CircularElement<E>> getQueue(StageRunner.SortStage stage) {
			switch (stage) {
				case READ:
					return empty;
				case SPILL:
					return spill;
				case SORT:
					return sort;
				default:
					throw new IllegalArgumentException();
			}
		}

		public CompletableFuture<MutableObjectIterator<E>> getIteratorFuture() {
			return iteratorFuture;
		}

		@Override
		public void send(StageRunner.SortStage stage, CircularElement<E> element) {
			getQueue(stage).add(element);
		}

		@Override
		public void sendResult(MutableObjectIterator<E> result) {
			iteratorFuture.complete(result);
		}

		@Override
		public CircularElement<E> take(StageRunner.SortStage stage) {
			try {
				return getQueue(stage).take();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public CircularElement<E> poll(StageRunner.SortStage stage) {
			return getQueue(stage).poll();
		}
	}
}
