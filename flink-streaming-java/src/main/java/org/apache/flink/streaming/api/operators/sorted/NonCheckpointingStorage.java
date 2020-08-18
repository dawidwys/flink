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

package org.apache.flink.streaming.api.operators.sorted;

import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * A dummy {@link CheckpointStorage} which does not perform checkpoints.
 */
public class NonCheckpointingStorage implements CheckpointStorage {
	@Override
	public boolean supportsHighlyAvailableStorage() {
		return false;
	}

	@Override
	public boolean hasDefaultSavepointLocation() {
		return false;
	}

	@Override
	public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
		throw new UnsupportedOperationException("Checkpoints are not supported in a single key state backend");
	}

	@Override
	public void initializeBaseLocations() throws IOException {

	}

	@Override
	public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId) throws IOException {
		throw new UnsupportedOperationException("Checkpoints are not supported in a single key state backend");
	}

	@Override
	public CheckpointStorageLocation initializeLocationForSavepoint(
			long checkpointId,
			@Nullable String externalLocationPointer) throws IOException {
		throw new UnsupportedOperationException("Checkpoints are not supported in a single key state backend");
	}

	@Override
	public CheckpointStreamFactory resolveCheckpointStorageLocation(
			long checkpointId,
			CheckpointStorageLocationReference reference) throws IOException {
		throw new UnsupportedOperationException("Checkpoints are not supported in a single key state backend");
	}

	@Override
	public CheckpointStreamFactory.CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException {
		throw new UnsupportedOperationException("Checkpoints are not supported in a single key state backend");
	}
}
