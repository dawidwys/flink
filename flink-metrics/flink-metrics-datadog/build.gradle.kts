/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation("com.squareup.okhttp3:okhttp:3.14.9")
    testImplementation(project(":flink-test-utils-junit"))
    testImplementation(project(":flink-metrics-core"))
    compileOnly(project(":flink-annotations"))
    compileOnly(project(":flink-metrics-core"))
    compileOnly("org.apache.flink:flink-shaded-jackson:2.12.4-14.0")
}

description = "Flink : Metrics : Datadog"
