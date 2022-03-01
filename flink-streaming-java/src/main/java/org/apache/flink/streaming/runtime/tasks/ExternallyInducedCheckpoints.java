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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Internal
@ThreadSafe
class ExternallyInducedCheckpoints {

    private final Object lock = new Object();

    @GuardedBy("lock")
    private final Map<Long, CheckpointParameters> triggeredCheckpoints = new HashMap<>();

    @GuardedBy("lock")
    private long lastTriggeredCheckpoint = -1;

    public boolean registerCheckpoint(CheckpointMetaData metaData, CheckpointOptions options) {
        synchronized (lock) {
            if (lastTriggeredCheckpoint >= metaData.getCheckpointId()) {
                return true;
            } else {
                triggeredCheckpoints.put(
                        metaData.getCheckpointId(),
                        new CheckpointParameters(options, metaData.getTimestamp()));
                return false;
            }
        }
    }

    public void declineCheckpoint(long checkpointId) {
        synchronized (lock) {
            triggeredCheckpoints.remove(checkpointId);
        }
    }

    public Optional<CheckpointParameters> triggerCheckpoint(long checkpointId) {
        synchronized (lock) {
            lastTriggeredCheckpoint = Math.max(lastTriggeredCheckpoint, checkpointId);
            if (lastTriggeredCheckpoint == checkpointId) {
                return Optional.ofNullable(triggeredCheckpoints.remove(checkpointId));
            }

            return Optional.empty();
        }
    }

    static class CheckpointParameters {
        private final CheckpointOptions options;
        private final long triggeringTime;

        CheckpointParameters(CheckpointOptions options, long triggeringTime) {
            this.options = options;
            this.triggeringTime = triggeringTime;
        }

        public CheckpointOptions getOptions() {
            return options;
        }

        public long getTriggeringTime() {
            return triggeringTime;
        }
    }
}
