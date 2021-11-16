/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-hadoop-compatibility_2.12"))
    implementation(project(":flink-hadoop-bulk"))
    implementation(project(":flink-connector-files"))
    implementation(project(":flink-orc_2.12"))
    implementation(project(":flink-orc-nohive_2.12"))
    implementation(project(":flink-parquet_2.12"))
    implementation(project(":flink-hadoop-fs"))
    testImplementation(project(":flink-table-common"))
    testImplementation(project(":flink-table-api-java"))
    testImplementation(project(":flink-table-planner_2.12"))
    testImplementation(project(":flink-java"))
    testImplementation(project(":flink-clients"))
    testImplementation("com.klarna:hiverunner:4.0.0")
    testImplementation("org.reflections:reflections:0.9.8")
    testImplementation("org.apache.hive:hive-service:2.3.4")
    testImplementation("org.apache.hive.hcatalog:hive-hcatalog-core:2.3.4")
    testImplementation("org.apache.hive.hcatalog:hive-webhcat-java-client:2.3.4")
    testImplementation(project(":flink-csv"))
    testImplementation(project(":flink-test-utils"))
    testImplementation("org.apache.derby:derby:10.10.2.0")
    testImplementation("org.apache.avro:avro:1.8.2")
    compileOnly(project(":flink-table-common"))
    compileOnly(project(":flink-table-api-java-bridge"))
    compileOnly(project(":flink-table-runtime_2.12"))
    compileOnly(project(":flink-table-planner_2.12"))
    compileOnly("org.apache.hadoop:hadoop-common:2.7.5")
    compileOnly("org.apache.hadoop:hadoop-mapreduce-client-core:2.7.5")
    compileOnly("org.apache.hive:hive-metastore:2.3.4")
    compileOnly("org.apache.hive:hive-exec:2.3.4")
}

description = "Flink : Connectors : Hive"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
