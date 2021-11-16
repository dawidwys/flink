/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation("org.apache.flink:flink-shaded-guava:30.1.1-jre-14.0")
    testImplementation(project(":flink-core"))
    testImplementation(project(":flink-test-utils"))
    testImplementation(project(":flink-streaming-java"))
    testImplementation(project(":flink-runtime"))
    testImplementation(project(":flink-statebackend-rocksdb"))
    providedCompile(project(":flink-core"))
    providedCompile(project(":flink-streaming-java"))
}

description = "Flink : Libraries : CEP"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
