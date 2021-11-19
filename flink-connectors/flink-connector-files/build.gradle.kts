/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    api(project(":flink-connector-base"))
    api(project(":flink-file-sink-common"))
    testImplementation(project(":flink-core"))
    testImplementation(project(":flink-connector-base"))
    testImplementation(project(":flink-test-utils"))
    testImplementation(project(":flink-connector-test-utils"))
    testImplementation(project(":flink-streaming-java"))
    testImplementation(project(":flink-streaming-java"))
    compileOnly(project(":flink-core"))
}

description = "Flink : Connectors : Files"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
