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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.io.AvailabilityProvider;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link SourceReader} that will be used once a {@link SourceOperator#finish()} is called. This
 * is necessary for stop-with-savepoint --drain to emulate a fully drained source.
 */
public class DrainedSourceReader<T, SplitT extends SourceSplit> implements SourceReader<T, SplitT> {
    @Override
    public void start() {}

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        return InputStatus.END_OF_INPUT;
    }

    @Override
    public List<SplitT> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return (CompletableFuture<Void>) AvailabilityProvider.AVAILABLE;
    }

    @Override
    public void addSplits(List<SplitT> splits) {}

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void close() throws Exception {}
}
