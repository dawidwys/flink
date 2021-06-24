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
 * We are performing aligned checkpoints with time out. We have seen at least a single aligned
 * barrier.
 */
final class AlternatingCollectingBarriers extends AbstractAlternatingAlignedBarrierHandlerState {

    AlternatingCollectingBarriers(ChannelState context) {
        super(context);
    }

    @Override
    public BarrierHandlerState alignmentTimeout(
            Controller controller, CheckpointBarrier checkpointBarrier)
            throws IOException, CheckpointException {
        state.prioritizeAllAnnouncements();
        CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
        controller.initInputsCheckpoint(unalignedBarrier);
        for (CheckpointableInput input : state.getInputs()) {
            input.checkpointStarted(unalignedBarrier);
        }
        controller.triggerGlobalCheckpoint(unalignedBarrier);
        return new AlternatingCollectingBarriersUnaligned(true, state);
    }

    @Override
    public BarrierHandlerState endOfChannelReceived(
            Controller controller, InputChannelInfo channelInfo) throws IOException {
        state.channelFinished(channelInfo);

        if (controller.allBarriersReceived()) {
            state.unblockAllChannels();
            controller.triggerGlobalCheckpoint(controller.getPendingCheckpoint());
            return new AlternatingWaitingForFirstBarrier(state.emptyState());
        }
        return this;
    }

    @Override
    protected BarrierHandlerState transitionAfterBarrierReceived(ChannelState state) {
        return this;
    }
}
