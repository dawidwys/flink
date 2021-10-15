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

package org.apache.flink.yarn;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link Utils}. */
public class UtilsTest extends TestLogger {

    private static final String YARN_RM_ARBITRARY_SCHEDULER_CLAZZ =
            "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler";

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testDeleteApplicationFiles() throws Exception {
        final Path applicationFilesDir = temporaryFolder.newFolder(".flink").toPath();
        Files.createFile(applicationFilesDir.resolve("flink.jar"));
        try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
            assertThat(files.count(), equalTo(1L));
        }
        try (Stream<Path> files = Files.list(applicationFilesDir)) {
            assertThat(files.count(), equalTo(1L));
        }

        Utils.deleteApplicationFiles(applicationFilesDir.toString());
        try (Stream<Path> files = Files.list(temporaryFolder.getRoot().toPath())) {
            assertThat(files.count(), equalTo(0L));
        }
    }

    @Test
    public void testGetUnitResource() {
        final int minMem = 64;
        final int minVcore = 1;
        final int incMem = 512;
        final int incVcore = 2;
        final int incMemLegacy = 1024;
        final int incVcoreLegacy = 4;

        YarnConfiguration yarnConfig = new YarnConfiguration();
        yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, minMem);
        yarnConfig.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES, minVcore);
        yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_MB_LEGACY_KEY, incMemLegacy);
        yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_VCORES_LEGACY_KEY, incVcoreLegacy);

        verifyUnitResourceVariousSchedulers(
                yarnConfig, minMem, minVcore, incMemLegacy, incVcoreLegacy);

        yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_MB_KEY, incMem);
        yarnConfig.setInt(Utils.YARN_RM_INCREMENT_ALLOCATION_VCORES_KEY, incVcore);

        verifyUnitResourceVariousSchedulers(yarnConfig, minMem, minVcore, incMem, incVcore);
    }

    @Test
    public void testSharedLibWithNonQualifiedPath() throws Exception {
        final String sharedLibPath = "/flink/sharedLib";
        final String nonQualifiedPath = "hdfs://" + sharedLibPath;
        final String defaultFs = "hdfs://localhost:9000";
        final String qualifiedPath = defaultFs + sharedLibPath;

        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(
                YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(nonQualifiedPath));
        final YarnConfiguration yarnConfig = new YarnConfiguration();
        yarnConfig.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, defaultFs);

        final List<org.apache.hadoop.fs.Path> sharedLibs =
                Utils.getQualifiedRemoteSharedPaths(flinkConfig, yarnConfig);
        assertThat(sharedLibs.size(), is(1));
        assertThat(sharedLibs.get(0).toUri().toString(), is(qualifiedPath));
    }

    @Test
    public void testSharedLibIsNotRemotePathShouldThrowException() throws IOException {
        final String localLib = "file:///flink/sharedLib";
        final Configuration flinkConfig = new Configuration();
        flinkConfig.set(YarnConfigOptions.PROVIDED_LIB_DIRS, Collections.singletonList(localLib));

        try {
            Utils.getQualifiedRemoteSharedPaths(flinkConfig, new YarnConfiguration());
            fail("We should throw an exception when the shared lib is set to local path.");
        } catch (FlinkException ex) {
            final String msg =
                    "The \""
                            + YarnConfigOptions.PROVIDED_LIB_DIRS.key()
                            + "\" should only "
                            + "contain dirs accessible from all worker nodes";
            assertThat(ex, FlinkMatchers.containsMessage(msg));
        }
    }

    @Test
    public void testGetYarnConfiguration() {
        final String flinkPrefix = "flink.yarn.";
        final String yarnPrefix = "yarn.";

        final String k1 = "brooklyn";
        final String v1 = "nets";

        final String k2 = "golden.state";
        final String v2 = "warriors";

        final String k3 = "miami";
        final String v3 = "heat";

        final Configuration flinkConfig = new Configuration();
        flinkConfig.setString(flinkPrefix + k1, v1);
        flinkConfig.setString(flinkPrefix + k2, v2);
        flinkConfig.setString(k3, v3);

        final YarnConfiguration yarnConfig = Utils.getYarnConfiguration(flinkConfig);

        assertEquals(v1, yarnConfig.get(yarnPrefix + k1, null));
        assertEquals(v2, yarnConfig.get(yarnPrefix + k2, null));
        assertTrue(yarnConfig.get(yarnPrefix + k3) == null);
    }

    @Test
    public void testGetTaskManagerShellCommand() {
        final Configuration cfg = new Configuration();
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                new TaskExecutorProcessSpec(
                        new CPUResource(1.0),
                        new MemorySize(0), // frameworkHeapSize
                        new MemorySize(0), // frameworkOffHeapSize
                        new MemorySize(111), // taskHeapSize
                        new MemorySize(0), // taskOffHeapSize
                        new MemorySize(222), // networkMemSize
                        new MemorySize(0), // managedMemorySize
                        new MemorySize(333), // jvmMetaspaceSize
                        new MemorySize(0), // jvmOverheadSize
                        Collections.emptyList());
        final ContaineredTaskManagerParameters containeredParams =
                new ContaineredTaskManagerParameters(
                        taskExecutorProcessSpec, new HashMap<String, String>());

        // no logging, with/out krb5
        final String java = "$JAVA_HOME/bin/java";
        final String jvmmem =
                "-Xmx111 -Xms111 -XX:MaxDirectMemorySize=222 -XX:MaxMetaspaceSize=333";
        final String jvmOpts = "-Djvm"; // if set
        final String tmJvmOpts = "-DtmJvm"; // if set
        final String logfile = "-Dlog.file=./logs/taskmanager.log"; // if set
        final String logback = "-Dlogback.configurationFile=file:./conf/logback.xml"; // if set
        final String log4j =
                "-Dlog4j.configuration=file:./conf/log4j.properties"
                        + " -Dlog4j.configurationFile=file:./conf/log4j.properties"; // if set
        final String mainClass = "org.apache.flink.runtime.clusterframework.BootstrapToolsTest";
        final String dynamicConfigs =
                TaskExecutorProcessUtils.generateDynamicConfigsStr(taskExecutorProcessSpec).trim();
        final String basicArgs = "--configDir ./conf";
        final String mainArgs = "-Djobmanager.rpc.address=host1 -Dkey.a=v1";
        final String args = dynamicConfigs + " " + basicArgs + " " + mainArgs;
        final String redirects = "1> ./logs/taskmanager.out 2> ./logs/taskmanager.err";

        assertEquals(
                java
                        + " "
                        + jvmmem
                        + ""
                        + // jvmOpts
                        ""
                        + // logging
                        " "
                        + mainClass
                        + " "
                        + dynamicConfigs
                        + " "
                        + basicArgs
                        + " "
                        + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        false,
                        false,
                        this.getClass(),
                        ""));

        assertEquals(
                java + " " + jvmmem + "" + // jvmOpts
                        "" + // logging
                        " " + mainClass + " " + args + " " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        false,
                        false,
                        this.getClass(),
                        mainArgs));

        final String krb5 = "-Djava.security.krb5.conf=krb5.conf";
        assertEquals(
                java + " " + jvmmem + " " + krb5 + // jvmOpts
                        "" + // logging
                        " " + mainClass + " " + args + " " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        false,
                        true,
                        this.getClass(),
                        mainArgs));

        // logback only, with/out krb5
        assertEquals(
                java + " " + jvmmem + "" + // jvmOpts
                        " " + logfile + " " + logback + " " + mainClass + " " + args + " "
                        + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        false,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + krb5 + // jvmOpts
                        " " + logfile + " " + logback + " " + mainClass + " " + args + " "
                        + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        false,
                        true,
                        this.getClass(),
                        mainArgs));

        // log4j, with/out krb5
        assertEquals(
                java + " " + jvmmem + "" + // jvmOpts
                        " " + logfile + " " + log4j + " " + mainClass + " " + args + " "
                        + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        true,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + krb5 + // jvmOpts
                        " " + logfile + " " + log4j + " " + mainClass + " " + args + " "
                        + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        false,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        // logback + log4j, with/out krb5
        assertEquals(
                java + " " + jvmmem + "" + // jvmOpts
                        " " + logfile + " " + logback + " " + log4j + " " + mainClass + " " + args
                        + " " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + krb5 + // jvmOpts
                        " " + logfile + " " + logback + " " + log4j + " " + mainClass + " " + args
                        + " " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        // logback + log4j, with/out krb5, different JVM opts
        cfg.setString(CoreOptions.FLINK_JVM_OPTIONS, jvmOpts);
        assertEquals(
                java + " " + jvmmem + " " + jvmOpts + " " + logfile + " " + logback + " " + log4j
                        + " " + mainClass + " " + args + " " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + jvmOpts + " " + krb5 + // jvmOpts
                        " " + logfile + " " + logback + " " + log4j + " " + mainClass + " " + args
                        + " " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        // logback + log4j, with/out krb5, different JVM opts
        cfg.setString(CoreOptions.FLINK_TM_JVM_OPTIONS, tmJvmOpts);
        assertEquals(
                java + " " + jvmmem + " " + jvmOpts + " " + tmJvmOpts + " " + logfile + " "
                        + logback + " " + log4j + " " + mainClass + " " + args + " " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        false,
                        this.getClass(),
                        mainArgs));

        assertEquals(
                java + " " + jvmmem + " " + jvmOpts + " " + tmJvmOpts + " " + krb5 + // jvmOpts
                        " " + logfile + " " + logback + " " + log4j + " " + mainClass + " " + args
                        + " " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        // now try some configurations with different yarn.container-start-command-template

        cfg.setString(
                ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
                "%java% 1 %jvmmem% 2 %jvmopts% 3 %logging% 4 %class% 5 %args% 6 %redirects%");
        assertEquals(
                java + " 1 " + jvmmem + " 2 " + jvmOpts + " " + tmJvmOpts + " " + krb5 + // jvmOpts
                        " 3 " + logfile + " " + logback + " " + log4j + " 4 " + mainClass + " 5 "
                        + args + " 6 " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));

        cfg.setString(
                ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
                "%java% %logging% %jvmopts% %jvmmem% %class% %args% %redirects%");
        assertEquals(
                java + " " + logfile + " " + logback + " " + log4j + " " + jvmOpts + " " + tmJvmOpts
                        + " " + krb5 + // jvmOpts
                        " " + jvmmem + " " + mainClass + " " + args + " " + redirects,
                Utils.getTaskManagerShellCommand(
                        cfg,
                        containeredParams,
                        "./conf",
                        "./logs",
                        true,
                        true,
                        true,
                        this.getClass(),
                        mainArgs));
    }

    private static void verifyUnitResourceVariousSchedulers(
            YarnConfiguration yarnConfig, int minMem, int minVcore, int incMem, int incVcore) {
        yarnConfig.set(YarnConfiguration.RM_SCHEDULER, Utils.YARN_RM_FAIR_SCHEDULER_CLAZZ);
        verifyUnitResource(yarnConfig, incMem, incVcore);

        yarnConfig.set(YarnConfiguration.RM_SCHEDULER, Utils.YARN_RM_SLS_FAIR_SCHEDULER_CLAZZ);
        verifyUnitResource(yarnConfig, incMem, incVcore);

        yarnConfig.set(YarnConfiguration.RM_SCHEDULER, YARN_RM_ARBITRARY_SCHEDULER_CLAZZ);
        verifyUnitResource(yarnConfig, minMem, minVcore);
    }

    private static void verifyUnitResource(
            YarnConfiguration yarnConfig, int expectedMem, int expectedVcore) {
        final Resource unitResource = Utils.getUnitResource(yarnConfig);
        assertThat(unitResource.getMemory(), is(expectedMem));
        assertThat(unitResource.getVirtualCores(), is(expectedVcore));
    }
}
