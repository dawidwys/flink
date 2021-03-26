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

final class WaitingForFirstBarrierUnaligned implements BarrierHandlerAction {

    private final boolean alternating;
    private final CheckpointableInput[] inputs;

    WaitingForFirstBarrierUnaligned(boolean alternating, CheckpointableInput[] inputs) {
        this.alternating = alternating;
        this.inputs = inputs;
    }

    @Override
    public BarrierHandlerAction alignmentTimeout(
            Context context, CheckpointBarrier checkpointBarrier) {
        // ignore already processing unaligned checkpoints
        return this;
    }

    @Override
    public BarrierHandlerAction announcementReceived(
            Context context, InputChannelInfo channelInfo, int sequenceNumber) throws IOException {
        inputs[channelInfo.getGateIdx()].convertToPriorityEvent(
                channelInfo.getInputChannelIdx(), sequenceNumber);
        return this;
    }

    @Override
    public BarrierHandlerAction barrierReceived(
            Context context, InputChannelInfo channelInfo, CheckpointBarrier checkpointBarrier)
            throws CheckpointException, IOException {
        // we received an out of order aligned barrier, we should resume consumption for the
        // channel, as it is being blocked by the credit-based network
        if (!checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint()) {
            inputs[channelInfo.getGateIdx()].resumeConsumption(channelInfo);
        }

        CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
        context.triggerTaskCheckpoint(unalignedBarrier);
        for (CheckpointableInput input : inputs) {
            input.checkpointStarted(unalignedBarrier);
        }
        context.triggerGlobalCheckpoint(unalignedBarrier);
        if (context.allBarriersReceived()) {
            for (CheckpointableInput input : inputs) {
                input.checkpointStopped(unalignedBarrier.getId());
            }
            if (alternating) {
                return new AlternatingWaitingForFirstBarrier(inputs);
            } else {
                return this;
            }
        }
        return new CollectingBarriersUnaligned(alternating, inputs);
    }

    @Override
    public BarrierHandlerAction abort(long cancelledId) {
        if (alternating) {
            return new AlternatingWaitingForFirstBarrier(inputs);
        } else {
            return this;
        }
    }
}
