/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

dependencies {
    implementation("org.apache.hadoop:hadoop-common:3.2.2")
    implementation(project(":flink-hadoop-fs"))
    implementation("com.amazonaws:aws-java-sdk-core:1.11.951")
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.951")
    implementation("com.amazonaws:aws-java-sdk-kms:1.11.951")
    implementation("com.amazonaws:aws-java-sdk-dynamodb:1.11.951")
    implementation("com.amazonaws:aws-java-sdk-sts:1.11.951")
    implementation("org.apache.hadoop:hadoop-aws:3.2.2")
    providedCompile(project(":flink-core"))
}

description = "Flink : FileSystems : S3 FS Base"
