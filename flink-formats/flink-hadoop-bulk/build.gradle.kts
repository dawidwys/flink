/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    testImplementation(project(":flink-test-utils"))
    testImplementation("org.apache.hadoop:hadoop-hdfs:2.4.1")
    testImplementation("org.apache.hadoop:hadoop-common:2.4.1")
    compileOnly(project(":flink-core"))
    compileOnly(project(":flink-file-sink-common"))
    compileOnly(project(":flink-streaming-java"))
    compileOnly("org.apache.hadoop:hadoop-common:2.4.1")
    compileOnly("org.apache.hadoop:hadoop-hdfs:2.4.1")
    compileOnly("org.apache.hadoop:hadoop-mapreduce-client-core:2.4.1")
}

description = "Flink : Formats : Hadoop bulk"
