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

package org.apache.flink.test.manual;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.SplittableIterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/**
 * This is for testing the performance of reduce, with different execution strategies.
 * (See also http://peel-framework.org/2016/04/07/hash-aggregations-in-flink.html)
 */
public class DataStreamReducePerformance {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void main() throws Exception {

		final int numElements = 40_000_000;
		final int keyRange    =  4_000_000;

		// warm up JIT
		testReducePerformance(new TupleIntIntIterator(1000),
			TupleTypeInfo.<Tuple2<Integer, Integer>>getBasicTupleTypeInfo(Integer.class, Integer.class),
			10000, false);

		// TupleIntIntIterator
		testReducePerformance(new TupleIntIntIterator(keyRange),
			TupleTypeInfo.<Tuple2<Integer, Integer>>getBasicTupleTypeInfo(Integer.class, Integer.class),
			numElements, true);

		// TupleStringIntIterator
		testReducePerformance(new TupleStringIntIterator(keyRange),
			TupleTypeInfo.<Tuple2<String, Integer>>getBasicTupleTypeInfo(String.class, Integer.class),
			numElements, true);
	}

	private <T, B extends CopyableIterator<T>> void testReducePerformance
		(B iterator, TypeInformation<T> typeInfo, int numRecords, boolean print) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.setStateBackend(new RocksDBStateBackend(temporaryFolder.newFolder().toURI()));
		env.getConfig().enableObjectReuse();

		@SuppressWarnings("unchecked")
		DataStream<T> output =
			env.fromParallelCollection(new SplittableRandomIterator<>(numRecords, iterator), typeInfo)
				.keyBy(0)
				.transform(
					"reducer",
					typeInfo,
					new SumReducer()
				);

		long start = System.currentTimeMillis();

		output.addSink(new RichSinkFunction<T>() {
			private IntCounter intCounter;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				intCounter = getRuntimeContext().getIntCounter("counter");
			}

			@Override
			public void invoke(T value, Context context) throws Exception {
				intCounter.add(1);
			}
		});
		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setGlobalDataExchangeMode(GlobalDataExchangeMode.POINTWISE_EDGES_PIPELINED);
		streamGraph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST);

		Integer count = env.execute(streamGraph).getAccumulatorResult("counter");
		System.out.println(count);

		long end = System.currentTimeMillis();
		if (print) {
			System.out.println("=== Time for " + iterator.getClass().getSimpleName() + " : " + (end - start) + "ms ===");
		}
	}

	private static final class SplittableRandomIterator<T, B extends CopyableIterator<T>> extends SplittableIterator<T> implements Serializable {

		private int numElements;
		private final B baseIterator;

		public SplittableRandomIterator(int numElements, B baseIterator) {
			this.numElements = numElements;
			this.baseIterator = baseIterator;
		}

		@Override
		public boolean hasNext() {
			return numElements > 0;
		}

		@Override
		public T next() {
			numElements--;
			return baseIterator.next();
		}

		@SuppressWarnings("unchecked")
		@Override
		public SplittableRandomIterator<T, B>[] split(int numPartitions) {
			int splitSize = numElements / numPartitions;
			int rem = numElements % numPartitions;
			SplittableRandomIterator<T, B>[] res = new SplittableRandomIterator[numPartitions];
			for (int i = 0; i < numPartitions; i++) {
				res[i] = new SplittableRandomIterator<T, B>(i < rem ? splitSize : splitSize + 1, (B) baseIterator.copy());
			}
			return res;
		}

		@Override
		public int getMaximumNumberOfSplits() {
			return numElements;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private interface CopyableIterator<T> extends Iterator<T> {
		CopyableIterator<T> copy();
	}

	private static final class TupleIntIntIterator implements CopyableIterator<Tuple2<Integer, Integer>>, Serializable {

		private final int keyRange;
		private Tuple2<Integer, Integer> reuse = new Tuple2<Integer, Integer>();

		private int rndSeed = 11;
		private Random rnd;

		public TupleIntIntIterator(int keyRange) {
			this.keyRange = keyRange;
			this.rnd = new Random(this.rndSeed);
		}

		public TupleIntIntIterator(int keyRange, int rndSeed) {
			this.keyRange = keyRange;
			this.rndSeed = rndSeed;
			this.rnd = new Random(rndSeed);
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Tuple2<Integer, Integer> next() {
			reuse.f0 = rnd.nextInt(keyRange);
			reuse.f1 = 1;
			return reuse;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public CopyableIterator<Tuple2<Integer, Integer>> copy() {
			return new TupleIntIntIterator(keyRange, rndSeed + rnd.nextInt(10000));
		}
	}

	private static final class TupleStringIntIterator implements CopyableIterator<Tuple2<String, Integer>>, Serializable {

		private final int keyRange;
		private Tuple2<String, Integer> reuse = new Tuple2<>();

		private int rndSeed = 11;
		private Random rnd;

		public TupleStringIntIterator(int keyRange) {
			this.keyRange = keyRange;
			this.rnd = new Random(this.rndSeed);
		}

		public TupleStringIntIterator(int keyRange, int rndSeed) {
			this.keyRange = keyRange;
			this.rndSeed = rndSeed;
			this.rnd = new Random(rndSeed);
		}

		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Tuple2<String, Integer> next() {
			reuse.f0 = String.valueOf(rnd.nextInt(keyRange));
			reuse.f1 = 1;
			return reuse;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public CopyableIterator<Tuple2<String, Integer>> copy() {
			return new TupleStringIntIterator(keyRange, rndSeed + rnd.nextInt(10000));
		}
	}

	private static final class SumReducer<K>
			extends AbstractStreamOperator<Tuple2<K, Integer>>
			implements OneInputStreamOperator<Tuple2<K, Integer>, Tuple2<K, Integer>>, BoundedOneInput{

		private ValueState<Integer> counter;

		@Override
		public void open() throws Exception {
			super.open();
			counter = getRuntimeContext().getState(new ValueStateDescriptor<>(
				"counter",
				BasicTypeInfo.INT_TYPE_INFO));
		}

		@Override
		public void processElement(StreamRecord<Tuple2<K, Integer>> element) throws Exception {
			Integer v = counter.value();
			Integer incomingValue = element.<Tuple2<K, Integer>>asRecord().getValue().f1;
			if (v != null) {
				v += incomingValue;
			} else {
				v = incomingValue;
			}
			counter.update(v);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void endInput() throws Exception {
			getKeyedStateBackend().getKeys("counter", VoidNamespace.INSTANCE)
				.forEach(
					key -> {
						try {
							setCurrentKey(key);
							output.collect(new StreamRecord<>(Tuple2.of((K) key, counter.value())));
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					}
				);
		}
	}
}
