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

package org.apache.flink.connector.base.source.reader.splitreader;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Collection;

/**
 * A {@link SplitReader} that can pause splits that are advancing too fast in event time compared to
 * other splits.
 *
 * @param <E> the element type.
 * @param <SplitT> the split type.
 */
@Experimental
public interface AlignedSplitReader<E, SplitT extends SourceSplit> extends SplitReader<E, SplitT> {
    /**
     * Align the watermarks of splits by temporarily halt consumption of some splits and resume
     * consumption of others.
     *
     * <p>Note that no other methods can be called in parallel (except {@link #wakeUp()}, so it's
     * fine to non-atomically update subscriptions. This method is simply providing connectors with
     * more expressive APIs the opportunity to update all subscriptions at once.
     *
     * @param splitsToPause the splits to pause
     * @param splitsToResume the splits to resume
     */
    void alignSplits(Collection<SplitT> splitsToPause, Collection<SplitT> splitsToResume);
}
