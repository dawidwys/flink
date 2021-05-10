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

final class WaitingForFirstBarrierUnaligned implements BarrierHandlerState {

    private final boolean alternating;
    private final ChannelState channelState;

    WaitingForFirstBarrierUnaligned(boolean alternating, ChannelState channelState) {
        this.alternating = alternating;
        this.channelState = channelState;
    }

    @Override
    public BarrierHandlerState alignmentTimeout(
            Controller controller, CheckpointBarrier checkpointBarrier) {
        // ignore already processing unaligned checkpoints
        return this;
    }

    @Override
    public BarrierHandlerState announcementReceived(
            Controller controller, InputChannelInfo channelInfo, int sequenceNumber)
            throws IOException {
        channelState.getInputs()[channelInfo.getGateIdx()].convertToPriorityEvent(
                channelInfo.getInputChannelIdx(), sequenceNumber);
        return this;
    }

    @Override
    public BarrierHandlerState barrierReceived(
            Controller controller,
            InputChannelInfo channelInfo,
            CheckpointBarrier checkpointBarrier)
            throws CheckpointException, IOException {

        // we received an out of order aligned barrier, we should resume consumption for the
        // channel, as it is being blocked by the credit-based network
        if (!checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint()) {
            channelState.blockChannel(channelInfo);
        }

        CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
        controller.initInputsCheckpoint(unalignedBarrier);
        for (CheckpointableInput input : channelState.getInputs()) {
            input.checkpointStarted(unalignedBarrier);
        }
        controller.triggerGlobalCheckpoint(unalignedBarrier);
        if (controller.allBarriersReceived()) {
            for (CheckpointableInput input : channelState.getInputs()) {
                input.checkpointStopped(unalignedBarrier.getId());
            }
            return stopCheckpoint();
        }
        return new CollectingBarriersUnaligned(alternating, channelState);
    }

    @Override
    public BarrierHandlerState abort(long cancelledId) throws IOException {
        return stopCheckpoint();
    }

    private BarrierHandlerState stopCheckpoint() throws IOException {
        channelState.unblockAllChannels();
        if (alternating) {
            return new AlternatingWaitingForFirstBarrier(channelState.getInputs());
        } else {
            return this;
        }
    }
}
