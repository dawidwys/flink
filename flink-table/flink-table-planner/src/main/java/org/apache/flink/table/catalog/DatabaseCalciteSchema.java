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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.plan.schema.TableSinkTable;
import org.apache.flink.table.plan.schema.TableSourceTable;
import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * A mapping between Flink catalog's database and Calcite's schema.
 * Tables are registered as tables in the schema.
 */
class DatabaseCalciteSchema implements Schema {
	private final boolean isStreamingMode;
	private final String catalogName;
	private final String databaseName;
	private final CatalogManager catalogManager;

	public DatabaseCalciteSchema(
			boolean isStreamingMode,
			String databaseName,
			String catalogName,
			CatalogManager catalogManager) {
		this.isStreamingMode = isStreamingMode;
		this.databaseName = databaseName;
		this.catalogName = catalogName;
		this.catalogManager = catalogManager;
	}

	@Override
	public Table getTable(String tableName) {
		return Optional.ofNullable(catalogManager.getTemporaryTables().get(ObjectIdentifier.of(catalogName, databaseName, tableName)))
			.map(t -> convertTable(new ObjectPath(databaseName, tableName), t, null))
			.orElseGet(() -> getPermanentTable(tableName));
	}

	private Table getPermanentTable(String tableName) {
		ObjectPath tablePath = new ObjectPath(databaseName, tableName);

		try {
			Catalog catalog = catalogManager.getCatalog(catalogName).get();
			if (!catalog.tableExists(tablePath)) {
				return null;
			}

			CatalogBaseTable table = catalog.getTable(tablePath);

			return convertTable(tablePath, table, catalog.getTableFactory().orElse(null));
		} catch (TableNotExistException | CatalogException e) {
			// TableNotExistException should never happen, because we are checking it exists
			// via catalog.tableExists
			throw new TableException(format(
				"A failure occurred when accessing table. Table path [%s, %s, %s]",
				catalogName,
				databaseName,
				tableName), e);
		}
	}

	private Table convertTable(ObjectPath tablePath, CatalogBaseTable table, @Nullable TableFactory tableFactory) {
		if (table instanceof QueryOperationCatalogView) {
			return QueryOperationCatalogViewTable.createCalciteTable(((QueryOperationCatalogView) table));
		} else if (table instanceof ConnectorCatalogTable) {
			return convertConnectorTable((ConnectorCatalogTable<?, ?>) table);
		} else if (table instanceof CatalogTable) {
			return convertCatalogTable(tablePath, (CatalogTable) table, tableFactory);
		} else {
			throw new TableException("Unsupported table type: " + table);
		}
	}

	private Table convertConnectorTable(ConnectorCatalogTable<?, ?> table) {
		Optional<TableSourceTable> tableSourceTable = table.getTableSource()
			.map(tableSource -> new TableSourceTable<>(
				tableSource,
				!table.isBatch(),
				FlinkStatistic.UNKNOWN()));
		if (tableSourceTable.isPresent()) {
			return tableSourceTable.get();
		} else {
			Optional<TableSinkTable> tableSinkTable = table.getTableSink()
				.map(tableSink -> new TableSinkTable<>(
					tableSink,
					FlinkStatistic.UNKNOWN()));
			if (tableSinkTable.isPresent()) {
				return tableSinkTable.get();
			} else {
				throw new TableException("Cannot convert a connector table " +
					"without either source or sink.");
			}
		}
	}

	private Table convertCatalogTable(ObjectPath tablePath, CatalogTable table, @Nullable TableFactory tableFactory) {
		final TableSource<?> tableSource;
		if (tableFactory != null) {
			if (tableFactory instanceof TableSourceFactory) {
				tableSource = ((TableSourceFactory) tableFactory).createTableSource(tablePath, table);
			} else {
				throw new TableException(
					"Cannot query a sink-only table. TableFactory provided by catalog must implement TableSourceFactory");
			}
		} else {
			tableSource = TableFactoryUtil.findAndCreateTableSource(table);
		}

		if (!(tableSource instanceof StreamTableSource)) {
			throw new TableException("Catalog tables support only StreamTableSource and InputFormatTableSource");
		}

		return new TableSourceTable<>(
			tableSource,
			// this means the TableSource extends from StreamTableSource, this is needed for the
			// legacy Planner. Blink Planner should use the information that comes from the TableSource
			// itself to determine if it is a streaming or batch source.
			isStreamingMode,
			FlinkStatistic.UNKNOWN()
		);
	}

	@Override
	public Set<String> getTableNames() {
		return Stream.concat(
			catalogManager.getCatalog(catalogName).map(c -> {
				try {
					return c.listTables(databaseName);
				} catch (DatabaseNotExistException e) {
					throw new CatalogException(e);
				}
			}).orElse(Collections.emptyList()).stream(),
			catalogManager.getTemporaryTables()
				.keySet()
				.stream()
				.filter(i -> i.getCatalogName().equals(catalogName) && i.getDatabaseName().equals(databaseName))
				.map(ObjectIdentifier::getObjectName)
		).collect(Collectors.toSet());
	}

	@Override
	public RelProtoDataType getType(String name) {
		return null;
	}

	@Override
	public Set<String> getTypeNames() {
		return new HashSet<>();
	}

	@Override
	public Collection<Function> getFunctions(String s) {
		return new HashSet<>();
	}

	@Override
	public Set<String> getFunctionNames() {
		return new HashSet<>();
	}

	@Override
	public Schema getSubSchema(String s) {
		return null;
	}

	@Override
	public Set<String> getSubSchemaNames() {
		return new HashSet<>();
	}

	@Override
	public Expression getExpression(SchemaPlus parentSchema, String name) {
		return Schemas.subSchemaExpression(parentSchema, name, getClass());
	}

	@Override
	public boolean isMutable() {
		return true;
	}

	@Override
	public Schema snapshot(SchemaVersion schemaVersion) {
		return this;
	}
}
