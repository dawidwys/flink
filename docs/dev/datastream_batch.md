---
title: "BATCH Execution Mode"
nav-id: datastream_batch
nav-parent_id: streaming
nav-pos: 1
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

The DataStream API supports different runtime execution modes from which you
can choose depending on the requirements of your use case and the
characteristics of your job.

There is the "classic" execution behavior of the DataStream API which we call
`STREAMING` execution mode. This should be used for unbounded jobs that require
continuous incremental processing and are expected to stay online indefinitely.

Additionally, there is a batch-style execution mode that we call `BATCH`
execution mode. This executes jobs in a way that is more reminiscent of batch
processing frameworks such as MapReduce. This should be used for bounded jobs
for which you have a known fixed input and which do not run continuously.

* This will be replaced by the TOC
{:toc}

## When can/should I use BATCH execution mode?

Go into boundedness of sources -> boundedness of jobs.

## Configuring BATCH execution mode

The execution mode can be configured via the `execution.runtime-mode` setting.
There are three possible values:

 - `STREAMING`: The classic DataStream execution mode
 - `BATCH`: Batch-style execution on the DataStream API
 - `AUTOMATIC`: Let the system decide based on the boundedness of the sources

 This can be configured either in the `flink-conf.yaml`, via command line
 parameters of `bin/flink run ...`, or programmatically when
 creating/configuring the `StreamExecutionEnvironment`.

 Here's how you can configure the execution mode via the command line:

 ```bash
 $ bin/flink run -Dexecution.runtime-mode=BATCH examples/streaming/WordCount.jar
 ```

 This example shows how you can configure the execution mode in code:

 ```java
Configuration config = new Configuration();
config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
 ```

## Execution Behavior

### Task Scheduling And Network Shuffle

### Event Time / Watermarks

### Processing Time

### Failure Recovery

## Important Considerations

In the `BATCH` execution mode some of regular `STREAMING` features might behave differently or they might not be supported

* [Checkpointing]({{site.baseurl}}/concepts/stateful-stream-processing.html#stateful-stream-processing)
 and in turn any checkpointing dependent operations do not work.
* Custom operators should be implemented with care, otherwise they might behave improperly.
* The "rolling" operations such as e.g. [reduce](http://localhost:4000/dev/stream/operators/#reduce)
 or [sum]({{site.baseurl}}/dev/stream/operators/#aggregation) are not "rolling" in the `BATCH` mode.
 They emit only the final result.
* Features unsupported:
    - [Broadcast State]({{ site.baseurl}}/dev/stream/state/broadcast_state.html)
    - [Iterations]({{ site.baseurl}}/dev/stream/operators/#iterate)

See also additional explanations in subsequent sections for key areas.

### Broadcast State/Pattern

The feature was introduced to allow users to implement use-cases where a “control” stream needs to
be broadcasted to all downstream tasks, and its broadcasted elements, e.g. rules, need to be applied
to all incoming elements from another stream.

In this pattern, Flink provides no guarantees about the order in which the inputs are read.
Use-cases like the one above make sense in the streaming world where jobs are expected to run for
a long period with input data that are not known in advance. In these settings, requirements
may change over time depending on the incoming data. 

In the batch world though, we believe that such use-cases do not make much sense, as the input
(both the elements and the control stream) are static and known in advance.

We plan to support a variation of that pattern for `BATCH` processing where the broadcast side is
processed first entirely in the future.


### Checkpointing

Fault tolerance for batch programs does not use checkpointing. Recovery happens by fully replaying
the streams. That is possible, because inputs are bounded. This pushes the cost more towards the
recovery, but makes the regular processing cheaper, because it avoids checkpoints.

It is important to remember that because there are no checkpoints, certain features such as e.g. 
[CheckpointListener]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/common/state/CheckpointListener.html) and
as a result Kafka's [EXACTLY_ONCE]({{site.baseurl}}/dev/connectors/kafka.html#kafka-producers-and-fault-tolerance)
mode or File Sink's [OnCheckpointRollingPolicy]({{ site.baseurl}}/dev/connectors/streamfile_sink.html#rolling-policy) 
won't work. If you need a transactional sink that works in a `BATCH` mode make sure it uses the Unified Sink API as proposed
in the [FLIP-143](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API).

You can still use all the [state primitives]({{site.baseurl}}/dev/stream/state/state.html#working-with-state)
which will be backed by in-memory data structures.

### Writing custom operators

It is important to remember the assumptions made for `BATCH` execution mode when writing a
custom operator. Otherwise, an operator that works just fine for `STREAMING` mode might produce
wrong results in `BATCH` mode. Operators are never scoped to a particular key which means they
see some properties of `BATCH` processing Flink tries to leverage. 

First of all you should not cache the last seen watermark within an operator. In `BATCH` mode we
process records key by key. In result the Watermark will switch from MAX_VALUE to MIN_VALUE between
each key. You should not assume that the Watermark will always be ascending in an operator. For the
same reasons timers will fire first in the key order and then in the timestamp order within each key.
Moreover, operations that change a key manually are not supported.

Multi input operators implement a custom strategy for choosing which input to read from. It is in order
to ensure entire key is processed before moving on to the next one. As a result, the 
[InputSelectable]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/operators/InputSelectable.html)
is not supported.

Lastly, see the considerations described above as they apply here as well.
