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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.util.WrappingRuntimeException;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Deque;
import java.util.Optional;

/**
 * The abstract class of {@link CompletedCheckpointStore}, which holds the {@link
 * SharedStateRegistry} and provides the registration of shared state.
 */
public abstract class AbstractCompleteCheckpointStore implements CompletedCheckpointStore {
    private final SharedStateRegistry sharedStateRegistry;

    private final Object lock = new Object();

    @GuardedBy("lock")
    private CompletedCheckpointStorageLocation locationToDeleteWhenEmpty;

    public AbstractCompleteCheckpointStore(SharedStateRegistry sharedStateRegistry) {
        this.sharedStateRegistry = sharedStateRegistry;
    }

    @Override
    public void deleteLocationWhenEmpty(CompletedCheckpointStorageLocation storageLocation) {
        this.locationToDeleteWhenEmpty = storageLocation;
    }

    @Override
    public SharedStateRegistry getSharedStateRegistry() {
        return sharedStateRegistry;
    }

    @Override
    public void shutdown(JobStatus jobStatus, CheckpointsCleaner checkpointsCleaner)
            throws Exception {
        if (jobStatus.isGloballyTerminalState()) {
            sharedStateRegistry.close();
        }
    }

    protected final void tryToDeleteClaimedLocation() {
        try {
            if (locationToDeleteWhenEmpty == null) {
                return;
            }
            CompletedCheckpointStorageLocation locationToDelete = null;
            synchronized (lock) {
                if (locationToDeleteWhenEmpty != null && locationToDeleteWhenEmpty.isEmpty()) {
                    locationToDelete = locationToDeleteWhenEmpty;
                    locationToDeleteWhenEmpty = null;
                }
            }
            if (locationToDelete != null) {
                locationToDelete.disposeStorageLocation();
            }
        } catch (IOException e) {
            throw new WrappingRuntimeException(e);
        }
    }

    /**
     * Unregister shared states that are no longer in use. Should be called after completing a
     * checkpoint (even if no checkpoint was subsumed, so that state added by an aborted checkpoints
     * and not used later can be removed).
     */
    protected void unregisterUnusedState(Deque<CompletedCheckpoint> unSubsumedCheckpoints) {
        findLowest(unSubsumedCheckpoints).ifPresent(sharedStateRegistry::unregisterUnusedState);
    }

    private static Optional<Long> findLowest(Deque<CompletedCheckpoint> unSubsumedCheckpoints) {
        for (CompletedCheckpoint p : unSubsumedCheckpoints) {
            if (!p.getProperties().isSavepoint()) {
                return Optional.of(p.getCheckpointID());
            }
        }
        return Optional.empty();
    }
}
