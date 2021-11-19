/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-runtime"))
    implementation(project(":flink-clients"))
    implementation(project(":flink-java"))
    implementation(project(":flink-rpc-core"))
    implementation("org.apache.flink:flink-shaded-netty:4.1.65.Final-14.0")
    implementation("org.apache.flink:flink-shaded-guava:30.1.1-jre-14.0")
    implementation("org.apache.flink:flink-shaded-jackson:2.12.4-14.0")
    testImplementation(project(":flink-test-utils-junit"))
    testImplementation(project(":flink-test-utils"))
    testImplementation(project(":flink-runtime"))
    testImplementation("org.apache.flink:flink-shaded-jackson-module-jsonSchema:2.12.4-14.0")
    testImplementation("commons-io:commons-io:2.11.0")
}

description = "Flink : Runtime web"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
