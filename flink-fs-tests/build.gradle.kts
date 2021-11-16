/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    testImplementation("org.apache.hadoop:hadoop-common:2.4.1")
    testImplementation("org.apache.hadoop:hadoop-hdfs:2.4.1")
    testImplementation(project(":flink-streaming-java"))
    testImplementation(project(":flink-examples-batch_2.12"))
    testImplementation(project(":flink-avro"))
    testImplementation(project(":flink-test-utils-junit"))
    testImplementation(project(":flink-test-utils"))
    testImplementation(project(":flink-runtime"))
    testImplementation(project(":flink-core"))
    testImplementation("org.apache.hadoop:hadoop-hdfs:2.4.1")
    testImplementation("org.apache.hadoop:hadoop-common:2.4.1")
}

description = "Flink : FileSystems : Tests"
