/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation("org.antlr:antlr4-runtime:4.7")
    implementation(project(":flink-core"))
    testImplementation(project(":flink-test-utils-junit"))
}

description = "Flink : Table : Code Splitter"
