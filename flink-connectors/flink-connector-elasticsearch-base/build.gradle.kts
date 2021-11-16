/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation("org.elasticsearch:elasticsearch:5.3.3")
    testImplementation(project(":flink-test-utils"))
    testImplementation(project(":flink-runtime"))
    testImplementation(project(":flink-streaming-java"))
    testImplementation(project(":flink-table-common"))
    testImplementation(project(":flink-json"))
    compileOnly(project(":flink-streaming-java"))
    compileOnly(project(":flink-table-api-java-bridge"))
}

description = "Flink : Connectors : Elasticsearch base"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
