/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.runtime.io.MultiInputStreamProcessorFactory;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamTask} for executing a {@link MultipleInputStreamOperator} and supporting
 * the {@link MultipleInputStreamOperator} to select input for reading.
 */
@Internal
public class MultipleInputStreamTask<OUT> extends StreamTask<OUT, MultipleInputStreamOperator<OUT>> {
	public MultipleInputStreamTask(Environment env) throws Exception {
		super(env, new MultiInputStreamProcessorFactory<>());
	}

	@Override
	public void init() throws Exception {
	}
}
