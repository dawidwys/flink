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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.EndOfInputAware;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link MultiInputSortingDataInputs}.
 */
public class MultiInputSortingDataInputsTest  {
	@Test
	public void simpleFixedLengthKeySorting() throws Exception {
		CollectingDataOutput<Object> collectingDataOutput = new CollectingDataOutput<>();
		List<StreamElement> elements = Arrays.asList(
			new StreamRecord<>(1, 3),
			new StreamRecord<>(1, 1),
			new StreamRecord<>(2, 1),
			new StreamRecord<>(2, 3),
			new StreamRecord<>(1, 2),
			new StreamRecord<>(2, 2)
		);
		CollectionDataInput<Integer> dataInput1 = new CollectionDataInput<>(elements, 0);
		CollectionDataInput<Integer> dataInput2 = new CollectionDataInput<>(elements, 1);
		KeySelector<Integer, Integer> keySelector = value -> value;
		try (MockEnvironment environment = MockEnvironment.builder().build()) {
			@SuppressWarnings("unchecked")
			MultiInputSortingDataInputs<Integer> sortingDataInput = new MultiInputSortingDataInputs<Integer>(
				new StreamTaskInput[]{dataInput1, dataInput2},
				new KeySelector[]{keySelector, keySelector},
				new TypeSerializer[]{new IntSerializer(), new IntSerializer()},
				new IntSerializer(),
				environment.getMemoryManager(),
				environment.getIOManager(),
				true,
				1.0,
				new Configuration(),
				new DummyInvokable()
			);

			try (StreamTaskInput<Object> input1 = sortingDataInput.getInput(0);
					StreamTaskInput<Object> input2 = sortingDataInput.getInput(1)) {

				MultipleInputSelectionHandler selectionHandler = new MultipleInputSelectionHandler(null, 2);
				StreamMultipleInputProcessor processor = new StreamMultipleInputProcessor(
					selectionHandler,
					new StreamOneInputProcessor[]{
						new StreamOneInputProcessor(
							input1,
							collectingDataOutput,
							new DummyOperatorChain()
						),
						new StreamOneInputProcessor(
							input2,
							collectingDataOutput,
							new DummyOperatorChain()
						)
					}
				);

				InputStatus inputStatus;
				do {
					inputStatus = processor.processInput();
				} while (inputStatus != InputStatus.END_OF_INPUT);
			}
		}

		assertThat(collectingDataOutput.events, equalTo(
			Arrays.asList(
				new StreamRecord<>(1, 1),
				new StreamRecord<>(1, 1),
				new StreamRecord<>(1, 2),
				new StreamRecord<>(1, 2),
				new StreamRecord<>(1, 3),
				new StreamRecord<>(1, 3),
				new StreamRecord<>(2, 1),
				new StreamRecord<>(2, 1),
				new StreamRecord<>(2, 2),
				new StreamRecord<>(2, 2),
				new StreamRecord<>(2, 3),
				Watermark.MAX_WATERMARK, // max watermark from one of the inputs
				new StreamRecord<>(2, 3),
				Watermark.MAX_WATERMARK // max watermark from the other input
			)
		));
	}

	private static class DummyOperatorChain implements EndOfInputAware {
		@Override
		public void endMainOperatorInput(int inputId) throws Exception {

		}
	}
}
