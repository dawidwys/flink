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

package org.apache.flink.api.connector.source.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/** A {@link WatermarkOutput} with some internal methods relevant only to the framework. */
@Internal
public interface InternalWatermarkOutput extends WatermarkOutput {

    /**
     * Returns the last emitted watermark. If no watermark can be emitted (= no watermark assigner),
     * this method returns {@link org.apache.flink.api.common.eventtime.Watermark#MAX_WATERMARK}.
     *
     * <p>The main intent of this method is to help to align the sources around the lowest watermark
     * emitted across source readers. Hence, for periodic watermarks, the returned watermark may
     * actually be larger than the last emitted watermark of the SourceOperator as this returns the
     * internally recorded watermark.
     */
    long getLastWatermark();
}
