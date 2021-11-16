/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation("org.apache.flink:flink-shaded-guava:30.1.1-jre-14.0")
    testImplementation(project(":flink-test-utils-junit"))
    testImplementation(project(":flink-dstl-dfs"))
    testImplementation(project(":flink-runtime"))
    testImplementation(project(":flink-statebackend-rocksdb"))
    testImplementation(project(":flink-statebackend-rocksdb"))
    testImplementation(project(":flink-streaming-java"))
    providedCompile(project(":flink-core"))
    providedCompile(project(":flink-runtime"))
}

description = "Flink : State backends : Changelog"
