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

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.Internal;

import java.util.Arrays;

@Internal
public final class MultipleInputStatusMaintainer {
	/**
	 * Stream status for the two inputs. We need to keep track for determining when
	 * to forward stream status changes downstream.
	 */
	private final StreamStatus[] streamStatuses;
	private final StreamStatusMaintainer combinedStatus;

	public MultipleInputStatusMaintainer(int inputCount, StreamStatusMaintainer combinedStatus) {
		streamStatuses = new StreamStatus[inputCount];
		this.combinedStatus = combinedStatus;
		Arrays.fill(streamStatuses, StreamStatus.ACTIVE);
	}

	public void emitStreamStatus(int index, StreamStatus streamStatus) {
		streamStatuses[index] = streamStatus;

		// check if we need to toggle the task's stream status
		if (!streamStatus.equals(combinedStatus.getStreamStatus())) {
			if (streamStatus.isActive()) {
				// we're no longer idle if at least one input has become active
				combinedStatus.toggleStreamStatus(StreamStatus.ACTIVE);
			} else if (allStreamStatusesAreIdle()) {
				combinedStatus.toggleStreamStatus(StreamStatus.IDLE);
			}
		}
	}

	private boolean allStreamStatusesAreIdle() {
		for (StreamStatus streamStatus : streamStatuses) {
			if (streamStatus.isActive()) {
				return false;
			}
		}
		return true;
	}

	public StreamStatusMaintainer getMaintainerForInput(int idx) {
		return new StreamStatusMaintainer() {
			@Override
			public void toggleStreamStatus(StreamStatus streamStatus) {
				MultipleInputStatusMaintainer.this.emitStreamStatus(idx, streamStatus);
			}

			@Override
			public StreamStatus getStreamStatus() {
				return MultipleInputStatusMaintainer.this.combinedStatus.getStreamStatus();
			}
		};
	}
}
