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

package org.apache.flink.table.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link TableSourceUtils}.
 */
public class TableSourceUtilsTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testFieldMappingReordered() {
		int[] indices = TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.STRING())
				.build().getTableColumns(),
			ROW(FIELD("f0", DataTypes.STRING()), FIELD("f1", DataTypes.BIGINT())),
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {1, 0}));
	}

	@Test
	public void testFieldMappingNonMatchingTypes() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Type STRING of table field 'f0' does not match with type " +
			"TIMESTAMP(3) of the field of the TableSource return type.");
		TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.TIMESTAMP(3))
				.build().getTableColumns(),
			ROW(FIELD("f0", DataTypes.STRING()), FIELD("f1", DataTypes.BIGINT())),
			Function.identity()
		);
	}

	@Test
	public void testFieldMappingLegacyCompositeType() {
		int[] indices = TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("f1", DataTypes.BIGINT())
				.field("f0", DataTypes.STRING())
				.build().getTableColumns(),
			TypeConversions.fromLegacyInfoToDataType(new TupleTypeInfo<>(Types.STRING, Types.LONG)),
			Function.identity()
		);

		assertThat(indices, equalTo(new int[] {1, 0}));
	}

	@Test
	public void testFieldMappingLegacyCompositeTypeWithRenaming() {
		int[] indices = TableSourceUtils.computePhysicalIndices(
			TableSchema.builder()
				.field("a", DataTypes.BIGINT())
				.field("b", DataTypes.STRING())
				.build().getTableColumns(),
			TypeConversions.fromLegacyInfoToDataType(new TupleTypeInfo<>(Types.STRING, Types.LONG)),
			str -> {
				switch (str) {
					case "a":
						return "f1";
					case "b":
						return "f0";
					default:
						throw new AssertionError();
				}
			}
		);

		assertThat(indices, equalTo(new int[]{1, 0}));
	}

	@Test
	public void testMappingWithBatchTimeAttributes() {
		TestTableSource tableSource = new TestTableSource(
			DataTypes.BIGINT(),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		int[] indices = TableSourceUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIMESTAMP)
				.field("proctime", Types.SQL_TIMESTAMP)
				.build().getTableColumns(),
			false,
			Function.identity()
		);

		assertThat(indices, equalTo(new int[]{0, -3, -4}));
	}

	@Test
	public void testMappingWithStreamTimeAttributes() {
		TestTableSource tableSource = new TestTableSource(
			DataTypes.BIGINT(),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		int[] indices = TableSourceUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIMESTAMP)
				.field("proctime", Types.SQL_TIMESTAMP)
				.build().getTableColumns(),
			true,
			Function.identity()
		);

		assertThat(indices, equalTo(new int[]{0, -1, -2}));
	}

	@Test
	public void testMappingWithStreamTimeAttributesFromCompositeType() {
		TestTableSource tableSource = new TestTableSource(
			ROW(FIELD("b", TIME()), FIELD("a", DataTypes.BIGINT())),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		int[] indices = TableSourceUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIMESTAMP)
				.field("proctime", Types.SQL_TIMESTAMP)
				.build().getTableColumns(),
			true,
			Function.identity()
		);

		assertThat(indices, equalTo(new int[]{1, -1, -2}));
	}

	@Test
	public void testWrongLogicalTypeForRowtimeAttribute() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"Rowtime field 'rowtime' has invalid type TIME(0). Rowtime attributes must be of a Timestamp family.");

		TestTableSource tableSource = new TestTableSource(
			DataTypes.BIGINT(),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		TableSourceUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIME)
				.field("proctime", Types.SQL_TIMESTAMP)
				.build().getTableColumns(),
			false,
			Function.identity()
		);
	}

	@Test
	public void testWrongLogicalTypeForProctimeAttribute() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage(
			"Proctime field 'proctime' has invalid type TIME(0). Proctime attributes must be of a Timestamp family.");

		TestTableSource tableSource = new TestTableSource(
			DataTypes.BIGINT(),
			Collections.singletonList("rowtime"),
			"proctime"
		);
		TableSourceUtils.computePhysicalIndicesOrTimeAttributeMarkers(
			tableSource,
			TableSchema.builder()
				.field("a", Types.LONG)
				.field("rowtime", Types.SQL_TIMESTAMP)
				.field("proctime", Types.SQL_TIME)
				.build().getTableColumns(),
			false,
			Function.identity()
		);
	}

	private static class TestTableSource
		implements TableSource<Object>, DefinedProctimeAttribute, DefinedRowtimeAttributes {

		private final DataType producedDataType;
		private final List<String> rowtimeAttributes;
		private final String proctimeAttribute;

		private TestTableSource(
				DataType producedDataType,
				List<String> rowtimeAttributes,
				String proctimeAttribute) {
			this.producedDataType = producedDataType;
			this.rowtimeAttributes = rowtimeAttributes;
			this.proctimeAttribute = proctimeAttribute;
		}

		@Nullable
		@Override
		public String getProctimeAttribute() {
			return proctimeAttribute;
		}

		@Override
		public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
			return rowtimeAttributes.stream()
				.map(attr -> new RowtimeAttributeDescriptor(attr, null, null))
				.collect(Collectors.toList());
		}

		@Override
		public DataType getProducedDataType() {
			return producedDataType;
		}

		@Override
		public TableSchema getTableSchema() {
			throw new UnsupportedOperationException("Should not be called");
		}
	}
}
