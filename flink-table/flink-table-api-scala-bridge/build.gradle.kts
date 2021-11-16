/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-table-api-scala_2.12"))
    implementation(project(":flink-scala_2.12"))
    implementation(project(":flink-streaming-scala_2.12"))
    testImplementation(project(":flink-table-common"))
    testImplementation(project(":flink-table-api-java"))
}

description = "Flink : Table : API Scala bridge"
