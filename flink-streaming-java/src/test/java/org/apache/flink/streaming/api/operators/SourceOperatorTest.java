/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.mocks.MockSourceReader;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSourceSplitSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.source.event.WatermarkAlignmentEvent;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.api.operators.source.TestingSourceOperator;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.util.CollectionUtil;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Unit test for {@link SourceOperator}. */
@SuppressWarnings("serial")
public class SourceOperatorTest {

    private static final int SUBTASK_INDEX = 1;
    private static final MockSourceSplit MOCK_SPLIT = new MockSourceSplit(1234, 10);

    private MockSourceReader mockSourceReader;
    private MockOperatorEventGateway mockGateway;
    private SourceOperator<Integer, MockSourceSplit> operator;

    @Before
    public void setup() throws Exception {
        this.mockSourceReader = new MockSourceReader(true, false);
        this.mockGateway = new MockOperatorEventGateway();
        this.operator =
                new TestingSourceOperator<Integer>(
                        mockSourceReader,
                        WatermarkStrategy.forGenerator(
                                context -> new EventAsTimestampWatermarkGenerator()),
                        new TestProcessingTimeService(),
                        mockGateway,
                        SUBTASK_INDEX,
                        5,
                        true /* emit progressive watermarks */);
        Environment env = getTestingEnvironment();
        this.operator.setup(
                new SourceOperatorStreamTask<Integer>(env),
                new MockStreamConfig(new Configuration(), 1),
                new MockOutput<>(new ArrayList<>()));
        this.operator.initializeState(
                new StreamTaskStateInitializerImpl(env, new MemoryStateBackend()));
    }

    @After
    public void cleanUp() throws Exception {
        operator.close();
        assertTrue(mockSourceReader.isClosed());
    }

    @Test
    public void testInitializeState() throws Exception {
        StateInitializationContext stateContext = getStateContext();
        operator.initializeState(stateContext);

        assertNotNull(
                stateContext
                        .getOperatorStateStore()
                        .getListState(SourceOperator.SPLITS_STATE_DESC));
    }

    @Test
    public void testOpen() throws Exception {
        // Initialize the operator.
        operator.initializeState(getStateContext());
        // Open the operator.
        operator.open();
        // The source reader should have been assigned a split.
        assertEquals(Collections.singletonList(MOCK_SPLIT), mockSourceReader.getAssignedSplits());
        // The source reader should have started.
        assertTrue(mockSourceReader.isStarted());

        // A ReaderRegistrationRequest should have been sent.
        assertEquals(1, mockGateway.getEventsSent().size());
        OperatorEvent operatorEvent = mockGateway.getEventsSent().get(0);
        assertTrue(operatorEvent instanceof ReaderRegistrationEvent);
        assertEquals(SUBTASK_INDEX, ((ReaderRegistrationEvent) operatorEvent).subtaskId());
    }

    @Test
    public void testStop() throws Exception {
        // Initialize the operator.
        operator.initializeState(getStateContext());
        // Open the operator.
        operator.open();
        // The source reader should have been assigned a split.
        assertEquals(Collections.singletonList(MOCK_SPLIT), mockSourceReader.getAssignedSplits());

        CollectingDataOutput<Integer> dataOutput = new CollectingDataOutput<>();
        assertEquals(DataInputStatus.NOTHING_AVAILABLE, operator.emitNext(dataOutput));
        assertFalse(operator.isAvailable());

        CompletableFuture<Void> sourceStopped = operator.stop(StopMode.DRAIN);
        assertTrue(operator.isAvailable());
        assertFalse(sourceStopped.isDone());
        assertEquals(DataInputStatus.END_OF_DATA, operator.emitNext(dataOutput));
        operator.finish();
        assertTrue(sourceStopped.isDone());
    }

    @Test
    public void testHandleAddSplitsEvent() throws Exception {
        operator.initializeState(getStateContext());
        operator.open();
        MockSourceSplit newSplit = new MockSourceSplit((2));
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(newSplit), new MockSourceSplitSerializer()));
        // The source reader should have been assigned two splits.
        assertEquals(Arrays.asList(MOCK_SPLIT, newSplit), mockSourceReader.getAssignedSplits());
    }

    @Test
    public void testHandleAddSourceEvent() throws Exception {
        operator.initializeState(getStateContext());
        operator.open();
        SourceEvent event = new SourceEvent() {};
        operator.handleOperatorEvent(new SourceEventWrapper(event));
        // The source reader should have been assigned two splits.
        assertEquals(Collections.singletonList(event), mockSourceReader.getReceivedSourceEvents());
    }

    @Test
    public void testSnapshotState() throws Exception {
        StateInitializationContext stateContext = getStateContext();
        operator.initializeState(stateContext);
        operator.open();
        MockSourceSplit newSplit = new MockSourceSplit((2));
        operator.handleOperatorEvent(
                new AddSplitEvent<>(
                        Collections.singletonList(newSplit), new MockSourceSplitSerializer()));
        operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));

        // Verify the splits in state.
        List<MockSourceSplit> splitsInState =
                CollectionUtil.iterableToList(operator.getReaderState().get());
        assertEquals(Arrays.asList(MOCK_SPLIT, newSplit), splitsInState);
    }

    @Test
    public void testNotifyCheckpointComplete() throws Exception {
        StateInitializationContext stateContext = getStateContext();
        operator.initializeState(stateContext);
        operator.open();
        operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));
        operator.notifyCheckpointComplete(100L);
        assertEquals(100L, (long) mockSourceReader.getCompletedCheckpoints().get(0));
    }

    @Test
    public void testNotifyCheckpointAborted() throws Exception {
        StateInitializationContext stateContext = getStateContext();
        operator.initializeState(stateContext);
        operator.open();
        operator.snapshotState(new StateSnapshotContextSynchronousImpl(100L, 100L));
        operator.notifyCheckpointAborted(100L);
        assertEquals(100L, (long) mockSourceReader.getAbortedCheckpoints().get(0));
    }

    @Test
    public void testSameAvailabilityFuture() {
        final CompletableFuture<?> initialFuture = operator.getAvailableFuture();
        final CompletableFuture<?> secondFuture = operator.getAvailableFuture();
        assertThat(initialFuture, not(sameInstance(AvailabilityProvider.AVAILABLE)));
        assertThat(secondFuture, sameInstance(initialFuture));
    }

    @Test
    public void testAlignment() throws Exception {
        operator.open();

        List<Integer> input = new ArrayList<>();
        input.add(42);
        input.add(54);
        input.add(63);

        MockSourceSplit split = new MockSourceSplit(0, 0, input.size());
        input.forEach(split::addRecord);
        mockSourceReader.addSplits(Collections.singletonList(split));
        assertTrue(operator.isAvailable());

        // fetch first element (this forces reader output to be created - before that alignment
        // events are no-op
        CollectingDataOutput<Integer> output = new CollectingDataOutput<>();
        List<StreamElement> expectedOutput = new ArrayList<>();
        assertThat(operator.emitNext(output), is(DataInputStatus.MORE_AVAILABLE));
        expectedOutput.add(new StreamRecord<>(42, TimestampAssigner.NO_TIMESTAMP));
        expectedOutput.add(new org.apache.flink.streaming.api.watermark.Watermark(42));
        assertThat(output.getEvents(), Matchers.contains(expectedOutput.toArray()));

        // Alignment event after record has been emitted
        operator.handleOperatorEvent(new WatermarkAlignmentEvent(1));
        CompletableFuture<?> availableFuture = operator.getAvailableFuture();
        assertFalse(availableFuture.isDone());

        // Alignment after, original future should complete
        operator.handleOperatorEvent(new WatermarkAlignmentEvent(43));
        assertTrue(availableFuture.isDone());
        assertTrue(operator.isAvailable());

        // fetch next element, this goes beyond max watermark and triggers unavailable
        assertThat(operator.emitNext(output), is(DataInputStatus.NOTHING_AVAILABLE));
        expectedOutput.add(new StreamRecord<>(54, TimestampAssigner.NO_TIMESTAMP));
        expectedOutput.add(new org.apache.flink.streaming.api.watermark.Watermark(54));
        assertThat(output.getEvents(), Matchers.contains(expectedOutput.toArray()));
        CompletableFuture<?> availableFuture2 = operator.getAvailableFuture();
        assertFalse(availableFuture2.isDone());

        // watermark did not progress enough
        operator.handleOperatorEvent(new WatermarkAlignmentEvent(45));
        assertFalse(availableFuture2.isDone());

        // ready to read more
        operator.handleOperatorEvent(new WatermarkAlignmentEvent(55));
        assertTrue(availableFuture2.isDone());

        // read last record and check if alignment does not interfere
        assertThat(operator.emitNext(output), is(DataInputStatus.NOTHING_AVAILABLE));
        expectedOutput.add(new StreamRecord<>(63, TimestampAssigner.NO_TIMESTAMP));
        expectedOutput.add(new org.apache.flink.streaming.api.watermark.Watermark(63));
        assertThat(output.getEvents(), Matchers.contains(expectedOutput.toArray()));
        assertFalse(operator.isAvailable());
    }

    // ---------------- helper methods -------------------------

    private StateInitializationContext getStateContext() throws Exception {
        // Create a mock split.
        byte[] serializedSplitWithVersion =
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        new MockSourceSplitSerializer(), MOCK_SPLIT);

        // Crate the state context.
        OperatorStateStore operatorStateStore = createOperatorStateStore();
        StateInitializationContext stateContext =
                new StateInitializationContextImpl(null, operatorStateStore, null, null, null);

        // Update the context.
        stateContext
                .getOperatorStateStore()
                .getListState(SourceOperator.SPLITS_STATE_DESC)
                .update(Collections.singletonList(serializedSplitWithVersion));

        return stateContext;
    }

    private OperatorStateStore createOperatorStateStore() throws Exception {
        MockEnvironment env = new MockEnvironmentBuilder().build();
        final AbstractStateBackend abstractStateBackend = new MemoryStateBackend();
        CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
        return abstractStateBackend.createOperatorStateBackend(
                env, "test-operator", Collections.emptyList(), cancelStreamRegistry);
    }

    private Environment getTestingEnvironment() {
        return new StreamMockEnvironment(
                new Configuration(),
                new Configuration(),
                new ExecutionConfig(),
                1L,
                new MockInputSplitProvider(),
                1,
                new TestTaskStateManager());
    }

    private static class EventAsTimestampWatermarkGenerator implements WatermarkGenerator<Integer> {

        @Override
        public void onEvent(Integer event, long eventTimestamp, WatermarkOutput output) {
            output.emitWatermark(new Watermark(event.longValue()));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }
}
