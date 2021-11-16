/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-core"))
    implementation(project(":flink-java"))
    implementation(project(":flink-queryable-state-client-java"))
    providedCompile(project(":flink-streaming-java"))
    providedCompile(project(":flink-statebackend-rocksdb"))
}

description = "Flink : E2E Tests : Queryable state"
