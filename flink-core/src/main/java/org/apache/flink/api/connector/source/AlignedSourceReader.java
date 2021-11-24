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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.eventtime.Watermark;

/**
 * Allows the {@link SourceReader} to only output data up to a specific watermark on a fine-grain
 * level.
 *
 * <p>Flink will already stop calling {@link #pollNext(ReaderOutput)} when the watermark of a source
 * reader exceeds the current max watermark. This interface allows implementations to partially
 * pause consumption when the same reader consumes multiple resources. For example, if the reader
 * consumes multiple partitions and they have different data rates, the reader can temporarily stop
 * consuming the low-volume partitions.
 *
 * <p>Note, it's totally fine if the reader returns data exceeding the watermark (max watermark in
 * the middle of the batch). The reader should not issue a new batch to those partitions that are
 * known to only contain data after the desired watermark.
 *
 * @param <T> The type of the record emitted by this source reader.
 * @param <SplitT> The type of the the source splits.
 */
@Experimental
public interface AlignedSourceReader<T, SplitT extends SourceSplit>
        extends SourceReader<T, SplitT> {
    void setCurrentMaxWatermark(Watermark watermark);
}
