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
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import static org.apache.flink.util.Preconditions.checkState;

/** Controller that can alternate between aligned and unaligned checkpoints. */
@Internal
public class AlternatingController implements CheckpointBarrierBehaviourController {

    private static final Logger LOG = LoggerFactory.getLogger(AlternatingController.class);

    private final AlignedController alignedController;
    private final UnalignedController unalignedController;
    private final DelayedActionRegistration delayedActionRegistration;
    private final Clock clock;

    private CheckpointBarrierBehaviourController activeController;
    private long firstBarrierArrivalTime = Long.MAX_VALUE;
    private long lastSeenBarrier = -1L;
    private long lastCompletedBarrier = -1L;

    public AlternatingController(
            AlignedController alignedController,
            UnalignedController unalignedController,
            Clock clock,
            DelayedActionRegistration delayedActionRegistration) {
        this.activeController = this.alignedController = alignedController;
        this.unalignedController = unalignedController;
        this.delayedActionRegistration = delayedActionRegistration;
        this.clock = clock;
    }

    @Override
    public void preProcessFirstBarrierOrAnnouncement(CheckpointBarrier barrier) {
        activeController = chooseController(barrier);
        activeController.preProcessFirstBarrierOrAnnouncement(barrier);
    }

    @Override
    public void barrierAnnouncement(
            InputChannelInfo channelInfo,
            CheckpointBarrier announcedBarrier,
            int sequenceNumber,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint)
            throws IOException, CheckpointException {
        if (lastSeenBarrier < announcedBarrier.getId()) {
            lastSeenBarrier = announcedBarrier.getId();
            firstBarrierArrivalTime = getArrivalTime(announcedBarrier);
            if (announcedBarrier.getCheckpointOptions().isTimeoutable()
                    && activeController == alignedController) {
                scheduleSwitchToUnaligned(announcedBarrier, triggerCheckpoint);
            }
        }

        Optional<CheckpointBarrier> maybeTimedOut = asTimedOut(announcedBarrier);
        announcedBarrier = maybeTimedOut.orElse(announcedBarrier);
        activeController.barrierAnnouncement(
                channelInfo, announcedBarrier, sequenceNumber, triggerCheckpoint);

        if (maybeTimedOut.isPresent() && activeController != unalignedController) {
            // Let's timeout this barrier
            switchToUnaligned(announcedBarrier, triggerCheckpoint);
        }
    }

    private void scheduleSwitchToUnaligned(
            CheckpointBarrier announcedBarrier,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint) {
        delayedActionRegistration.schedule(
                () -> {
                    long barrierId = announcedBarrier.getId();
                    if (lastSeenBarrier == barrierId
                            && lastCompletedBarrier < barrierId
                            && activeController == alignedController) {
                        // Let's timeout this barrier
                        LOG.info(
                                "Checkpoint alignment for barrier {} actively timed out. Switching"
                                        + " over to unaligned checkpoints.",
                                barrierId);
                        switchToUnaligned(announcedBarrier.asUnaligned(), triggerCheckpoint);
                    }
                    return null;
                },
                Duration.ofMillis(
                        announcedBarrier.getCheckpointOptions().getAlignmentTimeout() + 1));
    }

    private long getArrivalTime(CheckpointBarrier announcedBarrier) {
        if (announcedBarrier.getCheckpointOptions().isTimeoutable()) {
            return clock.relativeTimeNanos();
        } else {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public void barrierReceived(
            InputChannelInfo channelInfo,
            CheckpointBarrier barrier,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint)
            throws IOException, CheckpointException {
        if (barrier.getCheckpointOptions().isUnalignedCheckpoint()
                && activeController == alignedController) {
            switchToUnaligned(barrier, triggerCheckpoint);
        }

        Optional<CheckpointBarrier> maybeTimedOut = asTimedOut(barrier);
        barrier = maybeTimedOut.orElse(barrier);

        activeController.barrierReceived(
                channelInfo,
                barrier,
                checkpointBarrier -> {
                    throw new IllegalStateException("Control should not trigger a checkpoint");
                });

        if (maybeTimedOut.isPresent()) {
            if (activeController == alignedController) {
                LOG.info(
                        "Received a barrier for a checkpoint {} with timed out alignment. Switching"
                                + " over to unaligned checkpoints.",
                        barrier.getId());
                switchToUnaligned(maybeTimedOut.get(), triggerCheckpoint);
            } else {
                alignedController.resumeConsumption(channelInfo);
            }
        } else if (!barrier.getCheckpointOptions().isUnalignedCheckpoint()
                && activeController == unalignedController) {
            alignedController.resumeConsumption(channelInfo);
        }
    }

    @Override
    public void preProcessFirstBarrier(
            InputChannelInfo channelInfo,
            CheckpointBarrier barrier,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint)
            throws IOException, CheckpointException {
        if (lastSeenBarrier < barrier.getId()) {
            lastSeenBarrier = barrier.getId();
            firstBarrierArrivalTime = getArrivalTime(barrier);
        }
        activeController = chooseController(barrier);
        activeController.preProcessFirstBarrier(channelInfo, barrier, triggerCheckpoint);
    }

    private void switchToUnaligned(
            CheckpointBarrier barrier,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint)
            throws IOException, CheckpointException {
        checkState(alignedController == activeController);

        // timeout all not yet processed barriers for which alignedController has processed an
        // announcement
        Map<InputChannelInfo, Integer> announcedUnalignedBarriers =
                unalignedController.getSequenceNumberInAnnouncedChannels();
        for (Map.Entry<InputChannelInfo, Integer> entry :
                alignedController.getSequenceNumberInAnnouncedChannels().entrySet()) {
            InputChannelInfo unProcessedChannelInfo = entry.getKey();
            int announcedBarrierSequenceNumber = entry.getValue();
            if (announcedUnalignedBarriers.containsKey(unProcessedChannelInfo)) {
                checkState(
                        announcedUnalignedBarriers.get(unProcessedChannelInfo)
                                == announcedBarrierSequenceNumber);
            } else {
                unalignedController.barrierAnnouncement(
                        unProcessedChannelInfo,
                        barrier,
                        announcedBarrierSequenceNumber,
                        triggerCheckpoint);
            }
        }
        activeController = unalignedController;

        // get blocked channels before resuming consumption
        List<InputChannelInfo> blockedChannels = alignedController.getBlockedChannels();
        // alignedController might have already processed some barriers, so
        // "migrate"/forward those
        // calls to unalignedController.
        for (int i = 0; i < blockedChannels.size(); i++) {
            InputChannelInfo blockedChannel = blockedChannels.get(i);
            if (i == 0) {
                unalignedController.preProcessFirstBarrier(
                        blockedChannel, barrier, triggerCheckpoint);
            } else {
                unalignedController.barrierReceived(blockedChannel, barrier, triggerCheckpoint);
            }
        }

        alignedController.resumeConsumption();
    }

    @Override
    public void postProcessLastBarrier(
            InputChannelInfo channelInfo,
            CheckpointBarrier barrier,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint)
            throws IOException, CheckpointException {
        activeController.postProcessLastBarrier(channelInfo, barrier, triggerCheckpoint);
        if (lastCompletedBarrier < barrier.getId()) {
            lastCompletedBarrier = barrier.getId();
        }
    }

    @Override
    public void abortPendingCheckpoint(long cancelledId, CheckpointException exception)
            throws IOException {
        activeController.abortPendingCheckpoint(cancelledId, exception);
        if (lastCompletedBarrier < cancelledId) {
            lastCompletedBarrier = cancelledId;
        }
    }

    @Override
    public void obsoleteBarrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException {
        chooseController(barrier).obsoleteBarrierReceived(channelInfo, barrier);
    }

    private boolean isAligned(CheckpointBarrier barrier) {
        return barrier.getCheckpointOptions().needsAlignment();
    }

    private CheckpointBarrierBehaviourController chooseController(CheckpointBarrier barrier) {
        return isAligned(barrier) ? alignedController : unalignedController;
    }

    private Optional<CheckpointBarrier> asTimedOut(CheckpointBarrier barrier) {
        return Optional.of(barrier).filter(this::canTimeout).map(CheckpointBarrier::asUnaligned);
    }

    private boolean canTimeout(CheckpointBarrier barrier) {
        return barrier.getCheckpointOptions().isTimeoutable()
                && barrier.getId() <= lastSeenBarrier
                && barrier.getCheckpointOptions().getAlignmentTimeout() * 1_000_000
                        < (clock.relativeTimeNanos() - firstBarrierArrivalTime);
    }

    /** A provider for a method to register a delayed action. */
    @FunctionalInterface
    public interface DelayedActionRegistration {
        void schedule(Callable<?> callable, Duration delay);
    }
}
