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

/** The type of checkpoint to perform. */
public class SavepointType implements SnapshotType {

    private final String name;
    private final PostCheckpointAction postCheckpointAction;

    private SavepointType(final String name, final PostCheckpointAction postCheckpointAction) {

        this.postCheckpointAction = postCheckpointAction;
        this.name = name;
    }

    public static SavepointType savepoint() {
        return new SavepointType("Savepoint", PostCheckpointAction.NONE);
    }

    public static SavepointType terminate() {
        return new SavepointType("Terminate Savepoint", PostCheckpointAction.TERMINATE);
    }

    public static SavepointType suspend() {
        return new SavepointType("Suspend Savepoint", PostCheckpointAction.SUSPEND);
    }

    public boolean isSavepoint() {
        return true;
    }

    public boolean isSynchronous() {
        return postCheckpointAction != PostCheckpointAction.NONE;
    }

    public PostCheckpointAction getPostCheckpointAction() {
        return postCheckpointAction;
    }

    public boolean shouldAdvanceToEndOfTime() {
        return shouldDrain();
    }

    public boolean shouldDrain() {
        return getPostCheckpointAction() == PostCheckpointAction.TERMINATE;
    }

    public boolean shouldIgnoreEndOfInput() {
        return getPostCheckpointAction() == PostCheckpointAction.SUSPEND;
    }

    public String getName() {
        return name;
    }

    public SharingFilesStrategy getSharingFilesStrategy() {
        return SharingFilesStrategy.NO_SHARING;
    }

    /** What's the intended action after the checkpoint (relevant for stopping with savepoint). */
    public enum PostCheckpointAction {
        NONE,
        SUSPEND,
        TERMINATE
    }
}
