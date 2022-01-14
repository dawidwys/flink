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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.BulkDeletingFileSystem;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface BulkFileDeleter {
    void delete(Path fileToDelete) throws Exception;

    BulkFileDeleter IMMEDIATE_DELETER = path -> path.getFileSystem().delete(path, false);

    final class BulkFileDeleterImpl implements BulkFileDeleter, AutoCloseable {

        private final Map<String, List<Path>> filesByScheme = new HashMap<>();

        @Override
        public void delete(Path fileToDelete) throws Exception {
            filesByScheme
                    .computeIfAbsent(fileToDelete.toUri().getScheme(), k -> new ArrayList<>())
                    .add(fileToDelete);
        }

        @Override
        public void close() throws Exception {
            for (List<Path> filesForScheme : filesByScheme.values()) {
                if (!filesForScheme.isEmpty()) {
                    final FileSystem fileSystem = filesForScheme.get(0).getFileSystem();
                    if (fileSystem instanceof BulkDeletingFileSystem) {
                        ((BulkDeletingFileSystem) fileSystem).delete(filesForScheme);
                    } else {
                        // use closer to close all even if there are exceptions in between
                        try (Closer closer = Closer.create()) {
                            for (Path path : filesForScheme) {
                                closer.register(() -> fileSystem.delete(path, false));
                            }
                        }
                    }
                }
            }
        }
    }
}
