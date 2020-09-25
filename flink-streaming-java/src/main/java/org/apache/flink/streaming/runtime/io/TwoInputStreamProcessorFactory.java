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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.metrics.MinWatermarkGauge;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

public class TwoInputStreamProcessorFactory<IN1, IN2, OUT>
	extends AbstractStreamProcessorFactory<OUT, TwoInputStreamOperator<IN1, IN2, OUT>> {

	private static final Logger LOG = LoggerFactory.getLogger(MultiInputStreamProcessorFactory.class);

	@Override
	public StreamInputProcessor create(
			AbstractInvokable owner,
			StreamConfig config,
			Environment environment,
			TwoInputStreamOperator<IN1, IN2, OUT> operator,
			OperatorChain<OUT, ?> operatorChain,
			SubtaskCheckpointCoordinator checkpointCoordinator) {
		ClassLoader userClassLoader = environment.getUserCodeClassLoader().asClassLoader();

		TypeSerializer<IN1> inputDeserializer1 = config.getTypeSerializerIn1(userClassLoader);
		TypeSerializer<IN2> inputDeserializer2 = config.getTypeSerializerIn2(userClassLoader);

		int numberOfInputs = config.getNumberOfNetworkInputs();

		ArrayList<IndexedInputGate> inputList1 = new ArrayList<>();
		ArrayList<IndexedInputGate> inputList2 = new ArrayList<>();

		List<StreamEdge> inEdges = config.getInPhysicalEdges(userClassLoader);

		for (int i = 0; i < numberOfInputs; i++) {
			int inputType = inEdges.get(i).getTypeNumber();
			IndexedInputGate reader = environment.getInputGate(i);
			switch (inputType) {
				case 1:
					inputList1.add(reader);
					break;
				case 2:
					inputList2.add(reader);
					break;
				default:
					throw new RuntimeException("Invalid input type number: " + inputType);
			}
		}

		final WatermarkGauge input1WatermarkGauge = new WatermarkGauge();
		final WatermarkGauge input2WatermarkGauge = new WatermarkGauge();
		final MinWatermarkGauge minInputWatermarkGauge = new MinWatermarkGauge(
			input1WatermarkGauge,
			input2WatermarkGauge);

		operator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge);
		operator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_1_WATERMARK, input1WatermarkGauge);
		operator.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_2_WATERMARK, input2WatermarkGauge);
		// wrap watermark gauge since registered metrics must be unique
		environment.getMetricGroup().gauge(MetricNames.IO_CURRENT_INPUT_WATERMARK, minInputWatermarkGauge::getValue);

		return createInputProcessor(
			owner,
			environment,
			config,
			checkpointCoordinator,
			operator,
			operatorChain,
			inputList1,
			inputList2,
			inputDeserializer1,
			inputDeserializer2,
			input1WatermarkGauge,
			input2WatermarkGauge);
	}


	private StreamTwoInputProcessor<IN1, IN2> createInputProcessor(
			AbstractInvokable owner,
			Environment environment,
			StreamConfig config,
			SubtaskCheckpointCoordinator checkpointCoordinator,
			TwoInputStreamOperator<IN1, IN2, OUT> operator,
			OperatorChain<OUT, ?> operatorChain,
			List<IndexedInputGate> inputGates1,
			List<IndexedInputGate> inputGates2,
			TypeSerializer<IN1> inputDeserializer1,
			TypeSerializer<IN2> inputDeserializer2,
			WatermarkGauge input1WatermarkGauge,
			WatermarkGauge input2WatermarkGauge) {

		TwoInputSelectionHandler twoInputSelectionHandler = new TwoInputSelectionHandler(
			operator instanceof InputSelectable ? (InputSelectable) operator : null);

		// create an input instance for each input
		CheckpointedInputGate[] checkpointedInputGates = InputProcessorUtil.createCheckpointedMultipleInputGate(
			owner,
			config,
			checkpointCoordinator,
			environment.getMetricGroup().getIOMetricGroup(),
			getTaskNameWithSubtaskAndId(environment),
			inputGates1,
			inputGates2);
		checkState(checkpointedInputGates.length == 2);

		return new StreamTwoInputProcessor<>(
			checkpointedInputGates,
			inputDeserializer1,
			inputDeserializer2,
			environment.getIOManager(),
			operatorChain,
			operator,
			twoInputSelectionHandler,
			input1WatermarkGauge,
			input2WatermarkGauge,
			setupNumRecordsInCounter(operator));
	}
}
