/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-annotations"))
    implementation("org.apache.calcite:calcite-core:1.26.0")
    testImplementation("org.apache.calcite:calcite-core:1.26.0")
    compileOnly("com.google.guava:guava:29.0-jre")
}

description = "Flink : Table : SQL Parser"
