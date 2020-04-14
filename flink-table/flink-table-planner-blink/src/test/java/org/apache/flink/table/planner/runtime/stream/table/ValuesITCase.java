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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * End to end tests for {@link org.apache.flink.table.api.TableEnvironment#fromValues}.
 */
public class ValuesITCase extends StreamingTestBase {
	@Test
	public void testTypeConvesrsions() throws Exception {
		List<Row> data = Arrays.asList(
			Row.of(1, "ABC", java.sql.Timestamp.valueOf("2000-12-12 12:30:57.12"), Row.of(1, new byte[] {1, 2}, "ABC", Arrays.asList(1, 2, 3))),
			Row.of(Math.PI, "ABC", LocalDateTime.parse("2000-12-12T12:30:57.123456"), Row.of(Math.PI, new byte[]{2, 3}, "ABC", Arrays.asList(1L, 2L, 3L))),
			Row.of(3.1f, "DEF", LocalDateTime.parse("2000-12-12T12:30:57.1234567"), Row.of(3.1f, new byte[]{3}, "DEF", Arrays.asList(1D, 2D, 3D))),
			Row.of(99L, "DEFG", LocalDateTime.parse("2000-12-12T12:30:57.12345678"), Row.of(99L, new byte[]{3, 4}, "DEFG", Arrays.asList(1f, 2f, 3f))),
			Row.of(0d, "D", LocalDateTime.parse("2000-12-12T12:30:57.123"), Row.of(0d, new byte[]{4}, "D", Arrays.asList(1, 2, 3)))
		);

		DataType rowType = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.DECIMAL(10, 2).notNull()),
			DataTypes.FIELD("b", DataTypes.CHAR(4).notNull()),
			DataTypes.FIELD("c", DataTypes.TIMESTAMP(5).notNull()),
			DataTypes.FIELD(
				"row",
				DataTypes.ROW(
					DataTypes.FIELD("a", DataTypes.DECIMAL(10, 3)),
					DataTypes.FIELD("b", DataTypes.BINARY(2)),
					DataTypes.FIELD("c", DataTypes.CHAR(5).notNull()),
					DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.DECIMAL(10, 2))))
			)
		);

		Table t = tEnv().fromValues(
			rowType,
			data
		);

		TestCollectionTableFactory.reset();
		tEnv().sqlUpdate(
			"CREATE TABLE SinkTable(" +
				"a DECIMAL(10, 2) NOT NULL, " +
				"b CHAR(4) NOT NULL," +
				"c TIMESTAMP(4) NOT NULL," +
				"`row` ROW(a DECIMAL(10, 3) NOT NULL, b BINARY(2), c CHAR(5) NOT NULL, d ARRAY<DECIMAL(10, 2)>)) " +
				"WITH ('connector' = 'COLLECTION')");
		t.insertInto("SinkTable");
		tEnv().execute("");

		List<Row> expected = Arrays.asList(
			Row.of(new BigDecimal("1.00"), "ABC ", LocalDateTime.parse("2000-12-12T12:30:57.120"), Row.of(new BigDecimal("1.000"), new byte[] {1, 2}, "ABC  ", new BigDecimal[] { new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")})),
			Row.of(new BigDecimal("3.14"), "ABC ", LocalDateTime.parse("2000-12-12T12:30:57.123400"), Row.of(new BigDecimal("3.142"), new byte[] {2, 3}, "ABC  ", new BigDecimal[] { new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")})),
			Row.of(new BigDecimal("3.10"), "DEF ", LocalDateTime.parse("2000-12-12T12:30:57.123400"), Row.of(new BigDecimal("3.100"), new byte[] {3, 0}, "DEF  ", new BigDecimal[] { new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")})),
			Row.of(new BigDecimal("99.00"), "DEFG", LocalDateTime.parse("2000-12-12T12:30:57.123400"), Row.of(new BigDecimal("99.000"), new byte[] {3, 4}, "DEFG ", new BigDecimal[] { new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")})),
			Row.of(new BigDecimal("0.00"), "D   ", LocalDateTime.parse("2000-12-12T12:30:57.123"), Row.of(new BigDecimal("0.000"), new byte[] {4, 0}, "D    ", new BigDecimal[] { new BigDecimal("1.00"), new BigDecimal("2.00"), new BigDecimal("3.00")}))
		);

		List<Row> actual = TestCollectionTableFactory.getResult();
		assertThat(
			new HashSet<>(actual),
			equalTo(new HashSet<>(expected)));
	}

	@Test
	public void testAllTypes() throws Exception {
		List<Row> data = Arrays.asList(
			rowWithNestedRow(
				(byte) 1,
				(short) 1,
				1,
				1L,
				1.1f,
				1.1,
				new BigDecimal("1.1"),
				true,
				LocalTime.of(1, 1, 1),
				LocalDate.of(1, 1, 1),
				LocalDateTime.of(1, 1, 1, 1, 1, 1, 1),
				Instant.ofEpochMilli(1),
				"1",
				new byte[] {1},
				new BigDecimal[]{new BigDecimal("1.1")},
				createMap("1", new BigDecimal("1.1"))
			),
			rowWithNestedRow(
				(byte) 2,
				(short) 2,
				2,
				2L,
				2.2f,
				2.2,
				new BigDecimal("2.2"),
				false,
				LocalTime.of(2, 2, 2),
				LocalDate.of(2, 2, 2),
				LocalDateTime.of(2, 2, 2, 2, 2, 2, 2),
				Instant.ofEpochMilli(2),
				"2",
				new byte[] {2},
				new BigDecimal[]{new BigDecimal("2.2")},
				createMap("2", new BigDecimal("2.2"))
			)
		);

		Table t = tEnv().fromValues(data);

		TestCollectionTableFactory.reset();
		tEnv().sqlUpdate(
			"CREATE TABLE SinkTable(" +
				"f0 TINYINT, " +
				"f1 SMALLINT, " +
				"f2 INT, " +
				"f3 BIGINT, " +
				"f4 FLOAT, " +
				"f5 DOUBLE, " +
				"f6 DECIMAL(2, 1), " +
				"f7 BOOLEAN, " +
				"f8 TIME(0), " +
				"f9 DATE, " +
				"f12 TIMESTAMP(9), " +
				"f13 TIMESTAMP(3) WITH LOCAL TIME ZONE, " +
				"f14 CHAR(1), " +
				"f15 BINARY(1), " +
				"f16 ARRAY<DECIMAL(2, 1)>, " +
				"f17 MAP<CHAR(1), DECIMAL(2, 1)>, " +
				"f18 ROW(" +
				"   `f0` TINYINT, " +
				"   `f1` SMALLINT, " +
				"   `f2` INT, " +
				"   `f3` BIGINT, " +
				"   `f4` FLOAT, " +
				"   `f5` DOUBLE, " +
				"   `f6` DECIMAL(2, 1), " +
				"   `f7` BOOLEAN, " +
				"   `f8` TIME(0), " +
				"   `f9` DATE, " +
				"   `f12` TIMESTAMP(9), " +
				"   `f13` TIMESTAMP(3) WITH LOCAL TIME ZONE, " +
				"   `f14` CHAR(1), " +
				"   `f15` BINARY(1), " +
				"   `f16` ARRAY<DECIMAL(2, 1)>, " +
				"   `f17` MAP<CHAR(1), DECIMAL(2, 1)>)) " +
				"WITH ('connector' = 'COLLECTION')");
		t.insertInto("SinkTable");
		tEnv().execute("");

		List<Row> actual = TestCollectionTableFactory.getResult();
		assertThat(
			new HashSet<>(actual),
			equalTo(new HashSet<>(data)));
	}

	private static Map<String, BigDecimal> createMap(String key, BigDecimal value) {
		Map<String, BigDecimal> map = new HashMap<>();
		map.put(key, value);
		return map;
	}

	private static Row rowWithNestedRow(
			byte tinyint,
			short smallInt,
			int integer,
			long bigint,
			float floating,
			double doublePrecision,
			BigDecimal decimal,
			boolean bool,
			LocalTime time,
			LocalDate date,
//			Period dateTimeInteraval, // TODO INTERVAL types not supported in DDL yet
//			Duration timeInterval, // TODO INTERVAL types not supported in DDL yet
//			OffsetDateTime zonedDateTime, TODO TIMESTAMP WITH TIMEZONE not supported yet
			LocalDateTime timestamp,
			Instant localZonedTimestamp,
			String character,
			byte[] binary,
			BigDecimal[] array,
			Map<String, BigDecimal> map) {
		return Row.of(
			tinyint,
			smallInt,
			integer,
			bigint,
			floating,
			doublePrecision,
			decimal,
			bool,
			time,
			date,
			timestamp,
			localZonedTimestamp,
			character,
			binary,
			array,
			map,
			Row.of(
				tinyint,
				smallInt,
				integer,
				bigint,
				floating,
				doublePrecision,
				decimal,
				bool,
				time,
				date,
				timestamp,
				localZonedTimestamp,
				character,
				binary,
				array,
				map
			)
		);
	}
}
