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

What doesn't work:
 - broadcast state/pattern
 - iterations
 - operations that rely on checkpointing
 - this includes most "regular" sinks

