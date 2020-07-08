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

package org.apache.flink.runtime.operators.sort.v2;

import org.apache.flink.runtime.operators.sort.ExceptionHandler;

import java.io.IOException;

public interface SortStageRunner<E> {
	void start(
		String name,
		StageMessageDispatcher<E> dispatcher,
		ExceptionHandler<IOException> exceptionHandler) throws Exception;

	enum SortStage {
		READ,
		SPILL,
		SORT
	}

	interface StageMessageDispatcher<E> {
		void send(SortStage stage, CircularElement<E> element);
		CircularElement<E> take(SortStage stage);
		CircularElement<E> poll(SortStage stage);
	}
}
