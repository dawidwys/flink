/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.CloseableIterator;

import java.util.Iterator;

/**
 * General interface for state snapshots that should be written partitioned by key-groups. All
 * snapshots should be released after usage. This interface outlines the asynchronous snapshot
 * life-cycle, which typically looks as follows. In the synchronous part of a checkpoint, an
 * instance of {@link IterableStateSnapshot} is produced for a state and captures the state at this
 * point in time. Then, in the asynchronous part of the checkpoint, the user calls {@link
 * #getKeyGroupWriter()} to ensure that the snapshot is partitioned into key-groups. For state that
 * is already partitioned, this can be a NOP. The returned {@link StateKeyGroupWriter} can be used
 * by the caller to write the state by key-group. As a last step, when the state is completely
 * written, the user calls {@link #release()}.
 */
@Internal
public interface IterableStateSnapshot<K, N, S> extends StateSnapshot {
    Iterator<StateEntry<K, N, S>> getIterator(int keyGroup);
}
