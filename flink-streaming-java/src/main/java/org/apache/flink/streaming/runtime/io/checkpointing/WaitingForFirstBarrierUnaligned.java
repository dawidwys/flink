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

import static org.apache.flink.util.Preconditions.checkState;

/** @see AlternatingWaitingForFirstBarrierUnaligned */
final class WaitingForFirstBarrierUnaligned implements BarrierHandlerState {
    private final CheckpointableInput[] inputs;

    WaitingForFirstBarrierUnaligned(CheckpointableInput[] inputs) {
        this.inputs = inputs;
    }

    @Override
    public BarrierHandlerState alignmentTimeout(
            Controller controller, CheckpointBarrier checkpointBarrier) {
        throw new IllegalStateException(
                "We are running in an unaligned mode only. We should never receive alignment timeout");
    }

    @Override
    public BarrierHandlerState announcementReceived(
            Controller controller, InputChannelInfo channelInfo, int sequenceNumber)
            throws IOException {
        inputs[channelInfo.getGateIdx()].convertToPriorityEvent(
                channelInfo.getInputChannelIdx(), sequenceNumber);
        return this;
    }

    @Override
    public BarrierHandlerState barrierReceived(
            Controller controller,
            InputChannelInfo channelInfo,
            CheckpointBarrier checkpointBarrier)
            throws CheckpointException, IOException {
        checkState(checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint());

        CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
        controller.initInputsCheckpoint(unalignedBarrier);
        for (CheckpointableInput input : inputs) {
            input.checkpointStarted(unalignedBarrier);
        }
        controller.triggerGlobalCheckpoint(unalignedBarrier);
        if (controller.allBarriersReceived()) {
            for (CheckpointableInput input : inputs) {
                input.checkpointStopped(unalignedBarrier.getId());
            }
            return this;
        }
        return new CollectingBarriersUnaligned(inputs);
    }

    @Override
    public BarrierHandlerState abort(long cancelledId) {
        return this;
    }
}
