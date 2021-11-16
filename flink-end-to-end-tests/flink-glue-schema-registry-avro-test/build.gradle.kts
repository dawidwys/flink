/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-clients"))
    implementation(project(":flink-connector-kinesis"))
    implementation(project(":flink-streaming-kinesis-test"))
    implementation(project(":flink-avro"))
    implementation(project(":flink-avro-glue-schema-registry_2.12"))
    implementation("junit:junit:4.13.2")
    implementation(project(":flink-connector-kinesis"))
    implementation("org.apache.flink:flink-shaded-guava:30.1.1-jre-14.0")
    testImplementation(project(":flink-test-utils-junit"))
    compileOnly(project(":flink-streaming-java"))
}

description = "Flink : E2E Tests : Avro AWS Glue Schema Registry"
