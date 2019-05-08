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

package org.apache.flink.table.catalog;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;

import java.util.Collection;

/**
 * A concrete implementation of {@link CalciteSchema}
 * that maintains minimal state.
 */
public class SimpleSchema extends CalciteSchema {
	/** Creates a SimpleSchema.
	 *
	 * <p>Use {@link CalciteSchema#createRootSchema(boolean)}
	 * or {@link #add(String, Schema)}. */
	public SimpleSchema(CalciteSchema parent, Schema schema, String name) {
		super(parent, schema, name, null, null, null, null, null, null, null, null);
	}

	public void setCache(boolean cache) {
		throw new UnsupportedOperationException();
	}

	public CalciteSchema add(String name, Schema schema) {
		final CalciteSchema calciteSchema =
			new SimpleSchema(this, schema, name);
		subSchemaMap.put(name, calciteSchema);
		return calciteSchema;
	}

	protected CalciteSchema getImplicitSubSchema(String schemaName,
		boolean caseSensitive) {
		// Check implicit schemas.
		Schema s = schema.getSubSchema(schemaName);
		if (s != null) {
			return new SimpleSchema(this, s, schemaName);
		}
		return null;
	}

	protected TableEntry getImplicitTable(String tableName,
		boolean caseSensitive) {
		// Check implicit tables.
		Table table = schema.getTable(tableName);
		if (table != null) {
			return tableEntry(tableName, table);
		}
		return null;
	}

	protected TypeEntry getImplicitType(String name, boolean caseSensitive) {
		// Check implicit types.
		RelProtoDataType type = schema.getType(name);
		if (type != null) {
			return typeEntry(name, type);
		}
		return null;
	}

	protected void addImplicitSubSchemaToBuilder(
		ImmutableSortedMap.Builder<String, CalciteSchema> builder) {
		ImmutableSortedMap<String, CalciteSchema> explicitSubSchemas = builder.build();
		for (String schemaName : schema.getSubSchemaNames()) {
			if (explicitSubSchemas.containsKey(schemaName)) {
				// explicit subschema wins.
				continue;
			}
			Schema s = schema.getSubSchema(schemaName);
			if (s != null) {
				CalciteSchema calciteSchema = new SimpleSchema(this, s, schemaName);
				builder.put(schemaName, calciteSchema);
			}
		}
	}

	protected void addImplicitTableToBuilder(ImmutableSortedSet.Builder<String> builder) {
		builder.addAll(schema.getTableNames());
	}

	protected void addImplicitFunctionsToBuilder(
		ImmutableList.Builder<Function> builder,
		String name, boolean caseSensitive) {
		Collection<Function> functions = schema.getFunctions(name);
		if (functions != null) {
			builder.addAll(functions);
		}
	}

	protected void addImplicitFuncNamesToBuilder(ImmutableSortedSet.Builder<String> builder) {
		builder.addAll(schema.getFunctionNames());
	}

	@Override protected void addImplicitTypeNamesToBuilder(
		ImmutableSortedSet.Builder<String> builder) {
		builder.addAll(schema.getTypeNames());
	}

	protected void addImplicitTablesBasedOnNullaryFunctionsToBuilder(
		ImmutableSortedMap.Builder<String, Table> builder) {
		ImmutableSortedMap<String, Table> explicitTables = builder.build();

		for (String s : schema.getFunctionNames()) {
			// explicit table wins.
			if (explicitTables.containsKey(s)) {
				continue;
			}
			for (Function function : schema.getFunctions(s)) {
				if (function instanceof TableMacro
					&& function.getParameters().isEmpty()) {
					final Table table = ((TableMacro) function).apply(ImmutableList.of());
					builder.put(s, table);
				}
			}
		}
	}

	protected TableEntry getImplicitTableBasedOnNullaryFunction(String tableName,
		boolean caseSensitive) {
		Collection<Function> functions = schema.getFunctions(tableName);
		if (functions != null) {
			for (Function function : functions) {
				if (function instanceof TableMacro
					&& function.getParameters().isEmpty()) {
					final Table table = ((TableMacro) function).apply(ImmutableList.of());
					return tableEntry(tableName, table);
				}
			}
		}
		return null;
	}

	protected CalciteSchema snapshot(CalciteSchema parent, SchemaVersion version) {
		return this;
	}

	protected boolean isCacheEnabled() {
		return false;
	}

}

// End SimpleSchema.java
