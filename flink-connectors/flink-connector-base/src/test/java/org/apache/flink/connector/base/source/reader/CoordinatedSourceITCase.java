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

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.connector.base.source.reader.mocks.MockSplitEnumerator;
import org.apache.flink.connector.base.source.reader.splitreader.AlignedSplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.NumberSequenceIterator;

import org.junit.Ignore;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;

/** IT case for the {@link Source} with a coordinator. */
public class CoordinatedSourceITCase extends AbstractTestBase {

    @Test
    @Ignore("manual test")
    public void testAlignment() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(2000);
        env.setParallelism(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 10));

        DataStream<Long> eventStream =
                env.fromSource(
                        new SourceWithLaggingPartitions(),
                        WatermarkStrategy.<Long>forMonotonousTimestamps()
                                .withTimestampAssigner(new LongTimestampAssigner()),
                        "NumberSequenceSource");

        eventStream
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(
                        new ProcessAllWindowFunction<Long, Long, TimeWindow>() {
                            @Override
                            public void process(
                                    Context context, Iterable<Long> elements, Collector<Long> out)
                                    throws Exception {
                                long count = 0;
                                for (Long ignored : elements) {
                                    count++;
                                }
                                out.collect(count);
                            }
                        })
                .print();
        env.execute("Even time alignment test job");
    }

    private static class LongTimestampAssigner implements SerializableTimestampAssigner<Long> {
        @Override
        public long extractTimestamp(Long record, long recordTimeStamp) {
            return recordTimeStamp;
        }
    }

    @Test
    public void testEnumeratorReaderCommunication() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MockBaseSource source = new MockBaseSource(2, 10, Boundedness.BOUNDED);
        DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestingSource");
        executeAndVerify(env, stream, 20);
    }

    @Test
    public void testMultipleSources() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MockBaseSource source1 = new MockBaseSource(2, 10, Boundedness.BOUNDED);
        MockBaseSource source2 = new MockBaseSource(2, 10, 20, Boundedness.BOUNDED);
        DataStream<Integer> stream1 =
                env.fromSource(source1, WatermarkStrategy.noWatermarks(), "TestingSource1");
        DataStream<Integer> stream2 =
                env.fromSource(source2, WatermarkStrategy.noWatermarks(), "TestingSource2");
        executeAndVerify(env, stream1.union(stream2), 40);
    }

    @Test
    public void testEnumeratorCreationFails() throws Exception {
        OnceFailingToCreateEnumeratorSource.reset();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));
        final Source<Integer, ?, ?> source =
                new OnceFailingToCreateEnumeratorSource(2, 10, Boundedness.BOUNDED);
        final DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestingSource");
        executeAndVerify(env, stream, 20);
    }

    @Test
    public void testEnumeratorRestoreFails() throws Exception {
        OnceFailingToRestoreEnumeratorSource.reset();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));
        env.enableCheckpointing(10);

        final Source<Integer, ?, ?> source =
                new OnceFailingToRestoreEnumeratorSource(2, 10, Boundedness.BOUNDED);
        final DataStream<Integer> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "TestingSource");
        executeAndVerify(env, stream, 20);
    }

    @SuppressWarnings("serial")
    private void executeAndVerify(
            StreamExecutionEnvironment env, DataStream<Integer> stream, int numRecords)
            throws Exception {
        stream.addSink(
                new RichSinkFunction<Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        getRuntimeContext()
                                .addAccumulator("result", new ListAccumulator<Integer>());
                    }

                    @Override
                    public void invoke(Integer value, Context context) throws Exception {
                        getRuntimeContext().getAccumulator("result").add(value);
                    }
                });
        List<Integer> result = env.execute().getAccumulatorResult("result");
        Collections.sort(result);
        assertEquals(numRecords, result.size());
        assertEquals(0, (int) result.get(0));
        assertEquals(numRecords - 1, (int) result.get(result.size() - 1));
    }

    // ------------------------------------------------------------------------

    private static class OnceFailingToCreateEnumeratorSource extends MockBaseSource {

        private static final long serialVersionUID = 1L;
        private static boolean hasFailed;

        OnceFailingToCreateEnumeratorSource(
                int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
            super(numSplits, numRecordsPerSplit, boundedness);
        }

        @Override
        public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> createEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext) {
            if (!hasFailed) {
                hasFailed = true;
                throw new FlinkRuntimeException("Test Failure");
            }

            return super.createEnumerator(enumContext);
        }

        static void reset() {
            hasFailed = false;
        }
    }

    /**
     * A source with the following behavior:
     *
     * <ol>
     *   <li>It initially creates an enumerator that does not assign work, waits until the first
     *       checkpoint completes (which contains all work, because none is assigned, yet) and then
     *       triggers a global failure.
     *   <li>Upon restoring from the failure, the first attempt to restore the enumerator fails with
     *       an exception.
     *   <li>The next time to restore the enumerator succeeds and the enumerator works regularly.
     * </ol>
     */
    private static class OnceFailingToRestoreEnumeratorSource extends MockBaseSource {

        private static final long serialVersionUID = 1L;
        private static boolean hasFailed;

        OnceFailingToRestoreEnumeratorSource(
                int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
            super(numSplits, numRecordsPerSplit, boundedness);
        }

        @Override
        public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> createEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext) {

            final SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> enumerator =
                    super.createEnumerator(enumContext);

            if (hasFailed) {
                // after the failure happened, we proceed normally
                return enumerator;
            } else {
                // before the failure, we go with
                try {
                    final List<MockSourceSplit> splits = enumerator.snapshotState(1L);
                    return new NonAssigningEnumerator(splits, enumContext);
                } catch (Exception e) {
                    throw new FlinkRuntimeException(e.getMessage(), e);
                }
            }
        }

        @Override
        public SplitEnumerator<MockSourceSplit, List<MockSourceSplit>> restoreEnumerator(
                SplitEnumeratorContext<MockSourceSplit> enumContext,
                List<MockSourceSplit> checkpoint)
                throws IOException {
            if (!hasFailed) {
                hasFailed = true;
                throw new FlinkRuntimeException("Test Failure");
            }

            return super.restoreEnumerator(enumContext, checkpoint);
        }

        static void reset() {
            hasFailed = false;
        }

        /**
         * This enumerator does not assign work, so all state is in the checkpoint. After the first
         * checkpoint is complete, it triggers a global failure.
         */
        private static class NonAssigningEnumerator extends MockSplitEnumerator {

            private final SplitEnumeratorContext<?> context;

            NonAssigningEnumerator(
                    List<MockSourceSplit> splits, SplitEnumeratorContext<MockSourceSplit> context) {
                super(splits, context);
                this.context = context;
            }

            @Override
            public void addReader(int subtaskId) {
                // we do nothing here to make sure there is no progress
            }

            @Override
            public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
                // we do nothing here to make sure there is no progress
            }

            @Override
            public void notifyCheckpointComplete(long checkpointId) throws Exception {
                // This is a bit of a clumsy way to trigger a global failover from a coordinator.
                // This is safe, though, because per the contract, exceptions in the enumerator
                // handlers trigger a global failover.
                context.callAsync(
                        () -> null,
                        (success, failure) -> {
                            throw new FlinkRuntimeException(
                                    "Artificial trigger for Global Failover");
                        });
            }
        }
    }

    private static class SourceWithLaggingPartitions extends NumberSequenceSource {

        public static final int FROM = 0;
        public static final long TO = Long.MAX_VALUE;

        public SourceWithLaggingPartitions() {
            super(FROM, TO);
        }

        @Override
        public SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>>
                createEnumerator(final SplitEnumeratorContext<NumberSequenceSplit> enumContext) {

            final List<NumberSequenceSplit> splits = new ArrayList<>();

            for (int splitId = 0; splitId < 2 * enumContext.currentParallelism(); splitId++) {
                splits.add(new NumberSequenceSplit(String.valueOf(splitId), FROM, TO));
            }

            return new SourceWithLaggingPartitionEnumerator(splits, enumContext);
        }

        @Override
        public SourceReader<Long, NumberSequenceSplit> createReader(
                SourceReaderContext readerContext) {
            return new UnalignedSourceReader(readerContext);
        }

        private static class UnalignedSourceReader
                extends SingleThreadMultiplexSourceReaderBase<
                        Long, Long, NumberSequenceSplit, NumberSequenceSplit> {

            public UnalignedSourceReader(SourceReaderContext readerContext) {
                super(
                        UnalignedSourceSplitReader::new,
                        new UnalignedSourceRecordEmitter(),
                        readerContext.getConfiguration(),
                        readerContext);
            }

            @Override
            protected void onSplitFinished(Map<String, NumberSequenceSplit> finishedSplitIds) {}

            @Override
            protected NumberSequenceSplit initializedState(NumberSequenceSplit split) {
                return split;
            }

            @Override
            protected NumberSequenceSplit toSplitType(
                    String splitId, NumberSequenceSplit splitState) {
                return splitState;
            }
        }

        private static class UnalignedSourceRecordEmitter
                implements RecordEmitter<Long, Long, NumberSequenceSplit> {
            @Override
            public void emitRecord(
                    Long element, SourceOutput<Long> output, NumberSequenceSplit splitState) {
                output.collect(element, element);
            }
        }

        private static class UnalignedSourceSplitReader
                implements AlignedSplitReader<Long, NumberSequenceSplit> {
            private static final Duration SLOW_THROTTLE = Duration.ofMillis(1);
            private final Map<String, NumberSequenceIterator> slowSplits = new HashMap<>();
            private final Map<String, NumberSequenceIterator> fastSplits = new HashMap<>();
            private final Set<String> pausedSplitIds = new HashSet<>();
            private Deadline nextSlowEmission = Deadline.now();

            public UnalignedSourceSplitReader() {}

            @Override
            public void alignSplits(
                    Collection<NumberSequenceSplit> splitsToPause,
                    Collection<NumberSequenceSplit> splitsToResume) {
                for (NumberSequenceSplit split : splitsToPause) {
                    pausedSplitIds.add(split.splitId());
                }
                for (NumberSequenceSplit split : splitsToResume) {
                    pausedSplitIds.remove(split.splitId());
                }
            }

            @Override
            public RecordsWithSplitIds<Long> fetch() {
                Map<String, Collection<Long>> newValues = new HashMap<>();
                fastSplits.forEach(
                        (splitId, iter) -> {
                            if (!pausedSplitIds.contains(splitId)) {
                                newValues.put(splitId, singleton(iter.next()));
                            }
                        });
                if (!slowSplits.isEmpty() && nextSlowEmission.isOverdue()) {
                    slowSplits.forEach(
                            (splitId, iter) -> newValues.put(splitId, singleton(iter.next())));
                    nextSlowEmission = Deadline.fromNow(SLOW_THROTTLE);
                }

                return new RecordsBySplits<>(newValues, Collections.emptySet());
            }

            @Override
            public void handleSplitsChanges(SplitsChange<NumberSequenceSplit> splitsChanges) {
                splitsChanges.splits().stream()
                        .filter(s -> isSlow(s))
                        .forEach(s -> slowSplits.put(s.splitId(), s.getIterator()));
                splitsChanges.splits().stream()
                        .filter(s -> !isSlow(s))
                        .forEach(s -> fastSplits.put(s.splitId(), s.getIterator()));
            }

            private boolean isSlow(NumberSequenceSplit s) {
                return s.splitId().equals("1");
            }

            @Override
            public void wakeUp() {}

            @Override
            public void close() throws Exception {}
        }

        private static class SourceWithLaggingPartitionEnumerator
                implements SplitEnumerator<NumberSequenceSplit, Collection<NumberSequenceSplit>> {
            private final List<NumberSequenceSplit> splits;
            private final SplitEnumeratorContext<NumberSequenceSplit> enumContext;

            public SourceWithLaggingPartitionEnumerator(
                    List<NumberSequenceSplit> splits,
                    SplitEnumeratorContext<NumberSequenceSplit> enumContext) {
                this.splits = splits;
                this.enumContext = enumContext;
            }

            @Override
            public void start() {}

            @Override
            public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

            @Override
            public void addSplitsBack(List<NumberSequenceSplit> splits, int subtaskId) {}

            @Override
            public void addReader(int subtaskId) {
                for (int index = subtaskId;
                        index < splits.size();
                        index += enumContext.currentParallelism()) {
                    enumContext.assignSplit(splits.get(index), subtaskId);
                }
            }

            @Override
            public Collection<NumberSequenceSplit> snapshotState(long checkpointId)
                    throws Exception {
                return Collections.emptyList();
            }

            @Override
            public void close() throws IOException {}
        }
    }
}
