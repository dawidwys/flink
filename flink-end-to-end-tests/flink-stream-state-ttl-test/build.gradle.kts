/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-datastream-allround-test"))
    implementation(project(":flink-statebackend-rocksdb"))
    providedCompile(project(":flink-streaming-java"))
}

description = "Flink : E2E Tests : Stream state TTL"
