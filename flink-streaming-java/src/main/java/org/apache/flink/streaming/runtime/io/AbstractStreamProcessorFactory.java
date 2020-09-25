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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractStreamProcessorFactory<OUT, OP extends StreamOperator<OUT>>
	implements StreamInputProcessorFactory<OUT, OP> {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractStreamProcessorFactory.class);

	protected Counter setupNumRecordsInCounter(StreamOperator<?> streamOperator) {
		try {
			return ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
		} catch (Exception e) {
			LOG.warn("An exception occurred during the metrics setup.", e);
			return new SimpleCounter();
		}
	}

	/**
	 * Gets the name of the task, appended with the subtask indicator and execution id.
	 *
	 * @return The name of the task, with subtask indicator and execution id.
	 */
	protected String getTaskNameWithSubtaskAndId(Environment environment) {
		return environment.getTaskInfo().getTaskNameWithSubtasks() +
			" (" + environment.getExecutionId() + ')';
	}
}
