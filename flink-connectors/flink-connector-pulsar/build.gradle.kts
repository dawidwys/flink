/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-connector-base"))
    implementation("org.apache.pulsar:pulsar-client-all:2.8.0")
    testImplementation("org.assertj:assertj-core:3.20.2")
    testImplementation(project(":flink-core"))
    testImplementation(project(":flink-streaming-java"))
    testImplementation(project(":flink-tests"))
    testImplementation(project(":flink-connector-testing"))
    testImplementation(project(":flink-connector-test-utils"))
    testImplementation("org.testcontainers:pulsar:1.16.2")
    testImplementation("org.apache.pulsar:testmocks:2.8.0")
    testImplementation("org.apache.pulsar:pulsar-broker:2.8.0")
    testImplementation("org.apache.commons:commons-lang3:3.11")
    compileOnly(project(":flink-streaming-java"))
    compileOnly("com.google.protobuf:protobuf-java:3.17.3")
}

description = "Flink : Connectors : Pulsar"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
