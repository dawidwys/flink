/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation(project(":flink-core"))
    implementation(project(":flink-java"))
    implementation("org.apache.flink:flink-shaded-asm-7:7.1-14.0")
    implementation("org.scala-lang:scala-reflect:2.12.7")
    implementation("org.scala-lang:scala-library:2.12.7")
    implementation("org.scala-lang:scala-compiler:2.12.7")
    implementation("com.twitter:chill_2.12:0.7.6")
    testImplementation("org.scalatest:scalatest_2.12:3.0.0")
    testImplementation(project(":flink-test-utils-junit"))
    testImplementation(project(":flink-core"))
    testImplementation("joda-time:joda-time:2.5")
    testImplementation("org.joda:joda-convert:1.7")
}

description = "Flink : Scala"

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
