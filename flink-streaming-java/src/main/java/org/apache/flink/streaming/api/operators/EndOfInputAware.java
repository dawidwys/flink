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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;

/**
 * An interface for {@link OperatorChain} to extract the feature of ending the input.
 *
 * <p>It's purpose is mainly to make it easier to instantiate {@link StreamOneInputProcessor} which needs to
 * notify the chain that an input has ended.
 */
@Internal
public interface EndOfInputAware {

	/**
	 * Ends the main operator input specified by {@code inputId}).
	 *
	 * @param inputId the input ID starts from 1 which indicates the first input.
	 */
	void endMainOperatorInput(int inputId) throws Exception;
}
