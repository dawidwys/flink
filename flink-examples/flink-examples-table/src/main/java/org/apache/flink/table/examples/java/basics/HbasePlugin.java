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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;

public class HbasePlugin {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration configuration = new Configuration();
        configuration.set(TableConfigOptions.ENABLE_PLUGIN_MANAGER, parameterTool.has("plugins"));
        TableEnvironment tableEnvironment = TableEnvironment.create(configuration);

        tableEnvironment.executeSql(
                "CREATE TABLE KafkaTable (\n"
                        + "  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',\n"
                        + "  `partition` BIGINT METADATA VIRTUAL,\n"
                        + "  `offset` BIGINT METADATA VIRTUAL,\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `item_id` BIGINT,\n"
                        + "  `behavior` STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = 'user_behavior',\n"
                        + "  'properties.bootstrap.servers' = 'localhost:9092',\n"
                        + "  'properties.group.id' = 'testGroup',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  'format' = 'avro'\n"
                        + ")\n");

        tableEnvironment.from("`KafkaTable`").execute().print();
    }
}
