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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collection;

/**
 * An interface for components that can pause splits that are advancing too fast in event time
 * compared to other splits.
 */
@PublicEvolving
public interface WithSplitsAlignment {
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
    void alignSplits(Collection<String> splitsToPause, Collection<String> splitsToResume);
}
