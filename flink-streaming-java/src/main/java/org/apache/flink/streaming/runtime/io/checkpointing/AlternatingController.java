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

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import static org.apache.flink.util.Preconditions.checkState;

/** Controller that can alternate between aligned and unaligned checkpoints. */
@Internal
public class AlternatingController implements CheckpointBarrierBehaviourController {
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
            InputChannelInfo channelInfo, CheckpointBarrier announcedBarrier, int sequenceNumber)
            throws IOException {
        if (lastSeenBarrier < announcedBarrier.getId()) {
            lastSeenBarrier = announcedBarrier.getId();
            firstBarrierArrivalTime = getArrivalTime(announcedBarrier);
            if (announcedBarrier.getCheckpointOptions().isTimeoutable()
                    && activeController == alignedController) {
                scheduleAnnouncementTimeout(channelInfo, announcedBarrier, sequenceNumber);
            }
        }

        Optional<CheckpointBarrier> maybeTimedOut = asTimedOut(announcedBarrier);
        announcedBarrier = maybeTimedOut.orElse(announcedBarrier);

        if (maybeTimedOut.isPresent() && activeController != unalignedController) {
            // Let's timeout this barrier
            unalignedController.barrierAnnouncement(channelInfo, announcedBarrier, sequenceNumber);
        } else {
            // Either we have already timed out before, or we are still going with aligned
            // checkpoints
            activeController.barrierAnnouncement(channelInfo, announcedBarrier, sequenceNumber);
        }
    }

    private void scheduleAnnouncementTimeout(
            InputChannelInfo channelInfo, CheckpointBarrier announcedBarrier, int sequenceNumber) {
        delayedActionRegistration.schedule(
                () -> {
                    long barrierId = announcedBarrier.getId();
                    if (lastSeenBarrier == barrierId
                            && lastCompletedBarrier < barrierId
                            && activeController == alignedController) {
                        // Let's timeout this barrier
                        unalignedController.barrierAnnouncement(
                                channelInfo, announcedBarrier, sequenceNumber);
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
            switchToUnaligned(channelInfo, barrier, triggerCheckpoint);
            activeController.barrierReceived(channelInfo, barrier, triggerCheckpoint);
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
                switchToUnaligned(channelInfo, maybeTimedOut.get(), b -> {});
                triggerCheckpoint.accept(maybeTimedOut.get());
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
            if (barrier.getCheckpointOptions().isTimeoutable()
                    && activeController == alignedController) {
                scheduleSwitchToUnaligned(channelInfo, barrier, triggerCheckpoint);
            }
        }
        activeController = chooseController(barrier);
        activeController.preProcessFirstBarrier(channelInfo, barrier, triggerCheckpoint);
    }

    private void scheduleSwitchToUnaligned(
            InputChannelInfo channelInfo,
            CheckpointBarrier barrier,
            ThrowingConsumer<CheckpointBarrier, IOException> triggerCheckpoint) {
        delayedActionRegistration.schedule(
                () -> {
                    long barrierId = barrier.getId();
                    if (lastSeenBarrier == barrierId
                            && lastCompletedBarrier < barrierId
                            && activeController == alignedController) {
                        switchToUnaligned(channelInfo, barrier.asUnaligned(), triggerCheckpoint);
                    }
                    return null;
                },
                Duration.ofMillis(barrier.getCheckpointOptions().getAlignmentTimeout() + 1));
    }

    private void switchToUnaligned(
            InputChannelInfo channelInfo,
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
                        unProcessedChannelInfo, barrier, announcedBarrierSequenceNumber);
            }
        }

        // get blocked channels before resuming consumption
        Collection<InputChannelInfo> blockedChannels = alignedController.getBlockedChannels();

        activeController = unalignedController;

        // alignedController might have already processed some barriers, so "migrate"/forward those
        // calls to unalignedController.
        unalignedController.preProcessFirstBarrier(channelInfo, barrier, triggerCheckpoint);
        for (InputChannelInfo blockedChannel : blockedChannels) {
            unalignedController.barrierReceived(blockedChannel, barrier, triggerCheckpoint);
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
