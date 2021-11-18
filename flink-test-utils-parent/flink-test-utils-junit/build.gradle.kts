/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

description = "Flink : Test utils : Junit"

dependencies {
    api(libs.junit.jupiter)
    api(libs.junit.vintage)
    implementation(libs.log4j.api)
    implementation(libs.log4j.core)
    implementation(libs.log4j.slf4j)
}
