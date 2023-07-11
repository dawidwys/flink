package org.apache.flink.table.planner.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.factories.PlannerFactoryUtil;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.MutableURLClassLoader;

import org.junit.Test;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.bridge.internal.AbstractStreamTableEnvironmentImpl.lookupExecutor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for no default catalog and/or database.
 */
public class UnknownCatalogITCase {

    @Test
    public void testUsingUnknownCatalogWithFullyQualified() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tEnv = createTableEnvironment(env, EnvironmentSettings.inStreamingMode());
        final GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(
                "memory",
                ObjectIdentifier.UNKNOWN);
        catalog.createDatabase("db1", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
        tEnv.registerCatalog("memory", catalog);

        final String tablePath = "memory.db1.test";
        registerTable(tEnv, tablePath);

        // trigger translation
        Table table = tEnv.sqlQuery(String.format("SELECT * FROM %s", tablePath));

        assertThat(
                CollectionUtil.iteratorToList(table.execute().collect()).toString())
                .isEqualTo("[+I[1, a], +I[2, b]]");
    }

    @Test
    public void testUsingUnknownCatalogWithSingleIdentifier() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tEnv = createTableEnvironment(env, EnvironmentSettings.inStreamingMode());
        final GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(
                "memory",
                ObjectIdentifier.UNKNOWN);
        catalog.createDatabase("db1", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
        tEnv.registerCatalog("memory", catalog);

        final String tablePath = "memory.db1.test";
        registerTable(tEnv, tablePath);

        // trigger translation
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT * FROM test"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Object 'test' not found");
    }

    @Test
    public void testUsingUnknownDatabaseWithDatabaseQualified() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tEnv = createTableEnvironment(env, EnvironmentSettings.inStreamingMode());
        final GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(
                "memory",
                ObjectIdentifier.UNKNOWN);
        catalog.createDatabase("db1", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
        tEnv.registerCatalog("memory", catalog);
        tEnv.useCatalog("memory");

        final String tablePath = "memory.db1.test";
        registerTable(tEnv, tablePath);

        // trigger translation
        Table table = tEnv.sqlQuery("SELECT * FROM db1.test");

        assertThat(
                CollectionUtil.iteratorToList(table.execute().collect()).toString())
                .isEqualTo("[+I[1, a], +I[2, b]]");
    }

    @Test
    public void testUsingUnknownDatabaseWithSingleIdentifier() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tEnv = createTableEnvironment(env, EnvironmentSettings.inStreamingMode());
        final GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(
                "memory",
                ObjectIdentifier.UNKNOWN);
        catalog.createDatabase("db1", new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
        tEnv.registerCatalog("memory", catalog);
        tEnv.useCatalog("memory");

        final String tablePath = "memory.db1.test";
        registerTable(tEnv, tablePath);

        // trigger translation
        assertThatThrownBy(() -> tEnv.sqlQuery("SELECT * FROM test"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Object 'test' not found");
    }

    private static void registerTable(TableEnvironment tEnv, String tableName) {
        final String input1DataId =
                TestValuesTableFactory.registerData(Arrays.asList(Row.of(1, "a"), Row.of(2, "b")));
        tEnv.createTable(tableName, TableDescriptor.forConnector("values")
                .option("data-id", input1DataId)
                .schema(
                        Schema.newBuilder()
                                .column("i", INT())
                                .column("s", STRING())
                                .build())
                .build());
    }

    private static TableEnvironment createTableEnvironment(
            StreamExecutionEnvironment executionEnvironment,
            EnvironmentSettings settings) {
        final MutableURLClassLoader userClassLoader =
                FlinkUserCodeClassLoaders.create(
                        new URL[0], settings.getUserClassLoader(), settings.getConfiguration());
        final Executor executor = lookupExecutor(userClassLoader, executionEnvironment);

        final TableConfig tableConfig = TableConfig.getDefault();
        tableConfig.setRootConfiguration(executor.getConfiguration());
        tableConfig.addConfiguration(settings.getConfiguration());

        final ResourceManager resourceManager =
                new ResourceManager(settings.getConfiguration(), userClassLoader);
        final ModuleManager moduleManager = new ModuleManager();

        final CatalogManager catalogManager =
                CatalogManager.newBuilder()
                        .classLoader(userClassLoader)
                        .config(tableConfig)
                        .noDefaultCatalog()
                        .executionConfig(executionEnvironment.getConfig())
                        .build();

        final FunctionCatalog functionCatalog =
                new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager);

        final Planner planner =
                PlannerFactoryUtil.createPlanner(
                        executor,
                        tableConfig,
                        userClassLoader,
                        moduleManager,
                        catalogManager,
                        functionCatalog);

        return new StreamTableEnvironmentImpl(
                catalogManager,
                moduleManager,
                resourceManager,
                functionCatalog,
                tableConfig,
                executionEnvironment,
                planner,
                executor,
                settings.isStreamingMode());
    }
}
