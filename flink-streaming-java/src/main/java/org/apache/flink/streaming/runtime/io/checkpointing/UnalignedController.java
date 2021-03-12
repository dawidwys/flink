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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** Controller for unaligned checkpoints. */
@Internal
public class UnalignedController implements CheckpointBarrierBehaviourController {

    private final SubtaskCheckpointCoordinator checkpointCoordinator;
    private final CheckpointableInput[] inputs;
    private final Map<InputChannelInfo, Integer> sequenceNumberInAnnouncedChannels;

    public UnalignedController(
            SubtaskCheckpointCoordinator checkpointCoordinator, CheckpointableInput... inputs) {
        this.checkpointCoordinator = checkpointCoordinator;
        this.inputs = inputs;
        sequenceNumberInAnnouncedChannels = new HashMap<>();
    }

    @Override
    public void preProcessFirstBarrierOrAnnouncement(CheckpointBarrier barrier) {
        sequenceNumberInAnnouncedChannels.clear();
    }

    @Override
    public void barrierAnnouncement(
            InputChannelInfo channelInfo,
            CheckpointBarrier announcedBarrier,
            int sequenceNumber,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint)
            throws IOException {
        Preconditions.checkState(announcedBarrier.isCheckpoint());
        Integer previousValue = sequenceNumberInAnnouncedChannels.put(channelInfo, sequenceNumber);
        checkState(
                previousValue == null,
                "Stream corrupt: Repeated barrierAnnouncement [%s] overwriting [%s] for the same checkpoint on input %s",
                announcedBarrier,
                sequenceNumber,
                channelInfo);
        inputs[channelInfo.getGateIdx()].convertToPriorityEvent(
                channelInfo.getInputChannelIdx(), sequenceNumber);
    }

    @Override
    public void barrierReceived(
            InputChannelInfo channelInfo,
            CheckpointBarrier barrier,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint) {
        sequenceNumberInAnnouncedChannels.remove(channelInfo);
    }

    @Override
    public void preProcessFirstBarrier(
            InputChannelInfo channelInfo,
            CheckpointBarrier barrier,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint)
            throws IOException, CheckpointException {
        Preconditions.checkArgument(
                barrier.getCheckpointOptions().isUnalignedCheckpoint(),
                "Aligned barrier not expected");
        checkpointCoordinator.initCheckpoint(barrier.getId(), barrier.getCheckpointOptions());
        for (final CheckpointableInput input : inputs) {
            input.checkpointStarted(barrier);
        }
        triggerCheckpoint.accept(barrier);
    }

    @Override
    public void postProcessLastBarrier(
            InputChannelInfo channelInfo,
            CheckpointBarrier barrier,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint) {
        // note that barrier can be aligned if checkpoint timed out in between; event is not
        // converted
        resetPendingCheckpoint(barrier.getId());
    }

    private void resetPendingCheckpoint(long cancelledId) {
        for (final CheckpointableInput input : inputs) {
            input.checkpointStopped(cancelledId);
        }
    }

    @Override
    public void abortPendingCheckpoint(long cancelledId, CheckpointException exception) {
        resetPendingCheckpoint(cancelledId);
    }

    @Override
    public void obsoleteBarrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier) {}

    Map<InputChannelInfo, Integer> getSequenceNumberInAnnouncedChannels() {
        return Collections.unmodifiableMap(sequenceNumberInAnnouncedChannels);
    }
}
