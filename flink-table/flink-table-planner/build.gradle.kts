/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation("com.google.guava:guava:29.0-jre")
    implementation("org.codehaus.janino:commons-compiler:3.0.11")
    implementation("org.codehaus.janino:janino:3.0.11")
    implementation(project(":flink-table-common"))
    implementation(project(":flink-table-api-java"))
    implementation(project(":flink-table-api-scala_2.12"))
    implementation(project(":flink-table-api-java-bridge"))
    implementation(project(":flink-table-api-scala-bridge_2.12"))
    implementation(project(":flink-sql-parser"))
    implementation(project(":flink-sql-parser-hive"))
    implementation(project(":flink-table-runtime_2.12"))
    implementation("org.apache.calcite:calcite-core:1.26.0")
    implementation("com.ibm.icu:icu4j:67.1")
    implementation("org.scala-lang.modules:scala-parser-combinators_2.12:1.1.1")
    implementation("org.reflections:reflections:0.9.10")
    testImplementation(project(":flink-test-utils"))
    testImplementation(project(":flink-core"))
    testImplementation(project(":flink-tests"))
    testImplementation(project(":flink-table-common"))
    testImplementation(project(":flink-table-api-java"))
    testImplementation(project(":flink-table-runtime_2.12"))
    testImplementation(project(":flink-streaming-java"))
    testImplementation(project(":flink-statebackend-rocksdb"))
    providedCompile(project(":flink-scala_2.12"))
    providedCompile(project(":flink-streaming-scala_2.12"))
    providedCompile(project(":flink-cep"))
}

description = "Flink : Table : Planner"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
