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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.flink.runtime.state.testutils.NormalizedPathMatcher.normalizedPath;

/** Tests for configuring a {@link StreamTask}. */
public class StreamTaskConfigurationTest {

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    /** Tests that job parameters take precedence over cluster configurations. */
    @Test
    public void testConfigureJobManagerStorageWithParameters() throws Exception {
        final Path savepointDirJob = new Path(tmp.newFolder().toURI());
        final StreamConfig jobConfig = new StreamConfig(new Configuration());
        jobConfig.setSavepointDir(savepointDirJob);

        final String savepointDirConfig = new Path(tmp.newFolder().toURI()).toString();
        final Configuration clusterConfig = new Configuration();
        clusterConfig.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDirConfig);

        final TestStreamTask task =
                new TestStreamTask(
                        MockEnvironment.builder()
                                .setTaskConfiguration(jobConfig.getConfiguration())
                                .setTaskManagerRuntimeInfo(
                                        new TestingTaskManagerRuntimeInfo(clusterConfig))
                                .build());

        CheckpointStorage storage = task.getStorage();

        Assert.assertThat(storage, Matchers.instanceOf(JobManagerCheckpointStorage.class));
        JobManagerCheckpointStorage jmStorage = (JobManagerCheckpointStorage) storage;
        Assert.assertThat(jmStorage.getSavepointPath(), normalizedPath(savepointDirJob));
    }

    private static final class TestStreamTask extends StreamTask<Object, StreamOperator<Object>> {
        TestStreamTask(Environment env) throws Exception {
            super(env);
        }

        public CheckpointStorage getStorage() {
            return checkpointStorage;
        }

        @Override
        protected void init() throws Exception {}
    }
}
