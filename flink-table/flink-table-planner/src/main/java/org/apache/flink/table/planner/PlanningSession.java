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

package org.apache.flink.table.planner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkRelBuilderFactory;
import org.apache.flink.table.calcite.FlinkRelOptClusterFactory;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.calcite.FlinkTypeSystem;
import org.apache.flink.table.codegen.ExpressionReducer;
import org.apache.flink.table.expressions.ExpressionBridge;
import org.apache.flink.table.expressions.PlannerExpression;
import org.apache.flink.table.plan.TableOperationConverter;
import org.apache.flink.table.plan.cost.DataSetCostFactory;
import org.apache.flink.table.util.JavaScalaConversionUtil;
import org.apache.flink.table.validate.FunctionCatalog;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.Collections;

@Internal
public class PlanningSession {
	private static final RelOptCostFactory COST_FACTORY = new DataSetCostFactory();
	private static final RelDataTypeSystem TYPE_SYSTEM = new FlinkTypeSystem();
	private static final FlinkTypeFactory TYPE_FACTORY = new FlinkTypeFactory(TYPE_SYSTEM);
	private final RelOptPlanner planner;
	private final ExpressionBridge<PlannerExpression> expressionBridge;
	private final Context context;
	private final TableConfig tableConfig;
	private final FunctionCatalog functionCatalog;

	public PlanningSession(
			TableConfig tableConfig,
			FunctionCatalog functionCatalog,
			ExpressionBridge<PlannerExpression> expressionBridge) {
		this.tableConfig = tableConfig;
		this.functionCatalog = functionCatalog;
		// create context instances with Flink type factory
		this.planner = new VolcanoPlanner(COST_FACTORY, Contexts.empty());
		planner.setExecutor(new ExpressionReducer(tableConfig));
		planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

		this.expressionBridge = expressionBridge;

		this.context = Contexts.of(
			new TableOperationConverter.ToRelConverterSupplier(expressionBridge)
		);
	}

	/** Creates the [[FlinkRelBuilder]] of this TableEnvironment. */
	public FlinkRelBuilder createRelBuilder(SchemaPlus defaultSchema) {
		RelOptCluster cluster = FlinkRelOptClusterFactory.create(planner, new RexBuilder(TYPE_FACTORY));
		CalciteSchema calciteSchema = CalciteSchema.from(defaultSchema);
		RelOptSchema relOptSchema = new CalciteCatalogReader(
			calciteSchema,
			Collections.emptyList(),
			TYPE_FACTORY,
			CalciteConfig.connectionConfig(getSqlParserConfig(calciteConfig(tableConfig))));

		return new FlinkRelBuilder(context, cluster, relOptSchema, expressionBridge);
	}

	/** Returns the Calcite [[org.apache.calcite.plan.RelOptPlanner]] of this TableEnvironment. */
	public RelOptPlanner getPlanner() {
		return planner;
	}

	/** Returns the [[FlinkTypeFactory]] of this TableEnvironment. */
	public FlinkTypeFactory getTypeFactory() {
		return TYPE_FACTORY;
	}

	public Context getContext() {
		return context;
	}

	/** Returns the Calcite [[FrameworkConfig]] of this TableEnvironment. */
	public FrameworkConfig createFrameworkConfig(SchemaPlus defaultSchema) {
		return Frameworks
			.newConfigBuilder()
			.defaultSchema(defaultSchema)
			.parserConfig(getSqlParserConfig(calciteConfig(tableConfig)))
			.costFactory(COST_FACTORY)
			.typeSystem(TYPE_SYSTEM)
			.operatorTable(getSqlOperatorTable(calciteConfig(tableConfig), functionCatalog))
			.sqlToRelConverterConfig(getSqlToRelConverterConfig(calciteConfig(tableConfig), expressionBridge))
			// the converter is needed when calling temporal table functions from SQL, because
			// they reference a history table represented with a tree of table operations
			.context(context)
			// set the executor to evaluate constant expressions
			.executor(new ExpressionReducer(tableConfig))
			.build();
	}

	private CalciteConfig calciteConfig(TableConfig tableConfig) {
		return tableConfig.getPlannerConfig()
			.unwrap(CalciteConfig.class)
			.orElseGet(CalciteConfig::DEFAULT);
	}

	/**
	 * Returns the SqlToRelConverter config.
	 */
	private SqlToRelConverter.Config getSqlToRelConverterConfig(
		CalciteConfig calciteConfig,
		ExpressionBridge<PlannerExpression> expressionBridge) {
		return JavaScalaConversionUtil.toJava(calciteConfig.sqlToRelConverterConfig()).orElseGet(
			() -> SqlToRelConverter.configBuilder()
				.withTrimUnusedFields(false)
				.withConvertTableAccess(false)
				.withInSubQueryThreshold(Integer.MAX_VALUE)
				.withRelBuilderFactory(new FlinkRelBuilderFactory(expressionBridge))
				.build()
		);
	}

	/**
	 * Returns the operator table for this environment including a custom Calcite configuration.
	 */
	private SqlOperatorTable getSqlOperatorTable(CalciteConfig calciteConfig, FunctionCatalog functionCatalog) {
		return JavaScalaConversionUtil.toJava(calciteConfig.sqlOperatorTable()).map(operatorTable -> {
				if (calciteConfig.replacesSqlOperatorTable()) {
					return operatorTable;
				} else {
					return ChainedSqlOperatorTable.of(functionCatalog.getSqlOperatorTable(), operatorTable);
				}
			}
		).orElseGet(functionCatalog::getSqlOperatorTable);
	}

	/**
	 * Returns the SQL parser config for this environment including a custom Calcite configuration.
	 */
	private SqlParser.Config getSqlParserConfig(CalciteConfig calciteConfig) {
		return JavaScalaConversionUtil.toJava(calciteConfig.sqlParserConfig()).orElseGet(() ->
			// we use Java lex because back ticks are easier than double quotes in programming
			// and cases are preserved
			SqlParser
				.configBuilder()
				.setLex(Lex.JAVA)
				.build());
	}
}
