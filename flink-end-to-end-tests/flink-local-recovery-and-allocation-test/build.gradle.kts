/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    providedCompile(project(":flink-streaming-java"))
    providedCompile(project(":flink-statebackend-rocksdb"))
}

description = "Flink : E2E Tests : Local recovery and allocation"
