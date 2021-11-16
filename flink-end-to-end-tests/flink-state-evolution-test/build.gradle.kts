/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-avro"))
    compileOnly(project(":flink-java"))
    compileOnly(project(":flink-streaming-java"))
}

description = "Flink : E2E Tests : State evolution"
