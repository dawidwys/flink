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

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.io.IOException;

/**
 * Actions to be taken when processing aligned checkpoints and possibly switching to unaligned
 * checkpoints.
 */
abstract class AbstractAlternatingAlignedBarrierHandlerAction implements BarrierHandlerAction {

    private final AlignedCheckpointState state;

    protected AbstractAlternatingAlignedBarrierHandlerAction(AlignedCheckpointState state) {
        this.state = state;
    }

    @Override
    public final BarrierHandlerAction alignmentTimeout(
            Context context, CheckpointBarrier checkpointBarrier)
            throws IOException, CheckpointException {
        state.prioritizeAllAnnouncements();
        state.unblockAllChannels();
        CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
        context.triggerTaskCheckpoint(unalignedBarrier);
        for (CheckpointableInput input : state.getInputs()) {
            input.checkpointStarted(unalignedBarrier);
        }
        context.triggerGlobalCheckpoint(unalignedBarrier);
        return new CollectingBarriersUnaligned(true, state.getInputs());
    }

    @Override
    public final BarrierHandlerAction announcementReceived(
            Context context, InputChannelInfo channelInfo, int sequenceNumber) {
        state.addSeenAnnouncement(channelInfo, sequenceNumber);
        return this;
    }

    @Override
    public final BarrierHandlerAction barrierReceived(
            Context context, InputChannelInfo channelInfo, CheckpointBarrier checkpointBarrier)
            throws IOException, CheckpointException {
        if (checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint()) {
            BarrierHandlerAction unalignedState = alignmentTimeout(context, checkpointBarrier);
            return unalignedState.barrierReceived(context, channelInfo, checkpointBarrier);
        }

        state.removeSeenAnnouncement(channelInfo);
        state.blockChannel(channelInfo);
        if (context.allBarriersReceived()) {
            context.triggerGlobalCheckpoint(checkpointBarrier);
            state.unblockAllChannels();
            return new AlternatingWaitingForFirstBarrier(state.getInputs());
        } else if (context.isTimedOut(checkpointBarrier)
                || checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint()) {
            return alignmentTimeout(context, checkpointBarrier);
        }

        return transitionAfterBarrierReceived(state);
    }

    protected abstract BarrierHandlerAction transitionAfterBarrierReceived(
            AlignedCheckpointState state);

    protected abstract BarrierHandlerAction transitionAfterTimeout(AlignedCheckpointState state);

    @Override
    public final BarrierHandlerAction abort(long cancelledId) throws IOException {
        state.unblockAllChannels();
        return new AlternatingWaitingForFirstBarrier(state.getInputs());
    }
}
