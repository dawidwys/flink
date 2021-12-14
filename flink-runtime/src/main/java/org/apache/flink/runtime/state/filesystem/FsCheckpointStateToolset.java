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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.DuplicatingFileSystem;
import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;
import java.util.UUID;

public class FsCheckpointStateToolset implements CheckpointStateToolset {

    private final Path basePath;
    private final DuplicatingFileSystem fs;

    public FsCheckpointStateToolset(Path basePath, DuplicatingFileSystem fs) {
        this.basePath = basePath;
        this.fs = fs;
    }

    @Override
    public boolean canDuplicate(StreamStateHandle stateHandle) throws IOException {
        if (!(stateHandle instanceof FileStateHandle)) {
            return false;
        }
        final Path dst = getNewDstPath();
        return fs.canDuplicate(((FileStateHandle) stateHandle).getFilePath(), dst);
    }

    @Override
    public StreamStateHandle duplicate(StreamStateHandle stateHandle) throws IOException {

        if (!(stateHandle instanceof FileStateHandle)) {
            throw new IllegalArgumentException("We can duplicate only FileStateHandles.");
        }

        final Path dst = getNewDstPath();
        fs.duplicate(((FileStateHandle) stateHandle).getFilePath(), dst);

        if (stateHandle instanceof RelativeFileStateHandle) {
            return new RelativeFileStateHandle(dst, dst.getName(), stateHandle.getStateSize());
        } else {
            return new FileStateHandle(dst, stateHandle.getStateSize());
        }
    }

    private Path getNewDstPath() throws IOException {
        final String fileName = UUID.randomUUID().toString();
        final Path dst = new Path(basePath, fileName);
        return EntropyInjector.addEntropy(dst.getFileSystem(), dst);
    }
}
