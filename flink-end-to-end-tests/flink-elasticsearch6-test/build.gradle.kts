/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-connector-elasticsearch6"))
    compileOnly(project(":flink-streaming-java"))
}

description = "Flink : E2E Tests : Elasticsearch 6"
