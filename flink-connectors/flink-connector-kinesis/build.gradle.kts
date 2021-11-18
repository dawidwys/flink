import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
    id("org.apache.flink.java-conventions")
    id("com.github.johnrengelman.shadow") version "7.1.0"
}

dependencies {
    api(project(":flink-streaming-java"))
    compileOnly(project(":flink-runtime"))
    compileOnly("org.apache.flink:flink-table-api-java-bridge:1.15-SNAPSHOT")
    compileOnly(project(":flink-table-common"))

    implementation(libs.aws.kinesis)
    implementation(libs.aws.sts)
    implementation(libs.aws.s3)
    implementation(libs.aws.dynamodb)
    implementation(libs.aws.cloudwatch)
    implementation("com.amazonaws:amazon-kinesis-producer:0.14.1")
    implementation("com.amazonaws:amazon-kinesis-client:1.14.1")
    implementation("com.amazonaws:dynamodb-streams-kinesis-adapter:1.5.3")
    implementation("com.google.guava:guava:29.0-jre")
    implementation("software.amazon.awssdk:kinesis:2.16.86")
    implementation("software.amazon.awssdk:netty-nio-client:2.16.86")
    implementation("software.amazon.awssdk:sts:2.16.86")
    implementation("org.apache.httpcomponents:httpclient:4.5.13")
    implementation("org.apache.httpcomponents:httpcore:4.4.14")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.13.0")
    testImplementation(project(":flink-java"))
    testImplementation("org.apache.flink:flink-shaded-guava:30.1.1-jre-14.0")
    testImplementation("com.amazonaws:amazon-kinesis-aggregator:1.0.3")
    testImplementation(project(":flink-streaming-java", "testArtifacts"))
    testImplementation(project(":flink-runtime"))
    testImplementation(project(":flink-clients"))
    testImplementation(project(":flink-runtime", "testArtifacts"))
    testImplementation(project(":flink-core", "testArtifacts"))
    testImplementation("org.apache.flink:flink-table-api-java-bridge:1.15-SNAPSHOT") // TODO
    testImplementation("org.apache.flink:flink-table-runtime_2.12:1.15-SNAPSHOT") // TODO
    testImplementation("org.apache.flink:flink-tests:1.15-SNAPSHOT:tests") // TODO
    testImplementation(project(":flink-connector-testing"))
    testImplementation(project(":flink-test-utils"))
    testImplementation(project(":flink-test-utils-junit"))
    testImplementation("org.testcontainers:testcontainers:1.16.2")
    testImplementation(project(":flink-table-common"))
    testImplementation(project(":flink-table-common", "testArtifacts"))
    compileOnly("javax.xml.bind:jaxb-api:2.3.1")

    configurations.all {
        exclude(group = "com.amazonaws", module = "aws-java-sdk-cloudwatch")
        exclude(group = "software.amazon.ion")
    }

}

description = "Flink : Connectors : Kinesis"

tasks.withType<ShadowJar> {
    dependencies {
        include(dependency("com.amazonaws:"))
        include(dependency("com.google.protobuf:"))
        include(dependency("org.apache.httpcomponents:"))
        include(dependency("software.amazon.awssdk:"))
        include(dependency("software.amazon.eventstream:"))
        include(dependency("software.amazon.ion:"))
        include(dependency("org.reactivestreams:"))
        include(dependency("io.netty:"))
        include(dependency("com.typesafe.netty:"))
    }

    exclude(".gitkeep")
    exclude("META-INF/THIRD_PARTY_NOTICES")
    exclude("META-INF/NOTICE.txt")
    exclude("META-INF/maven/?*/?*/**")

    relocate("com.google.protobuf", "org.apache.flink.kinesis.shaded.com.google.protobuf")
    relocate("com.amazonaws", "org.apache.flink.kinesis.shaded.com.amazonaws")
    relocate("org.apache.http", "org.apache.flink.kinesis.shaded.org.apache.http")
    relocate("software.amazon", "org.apache.flink.kinesis.shaded.software.amazonaws")
    relocate("io.netty", "org.apache.flink.kinesis.shaded.io.netty")
    relocate("com.typesafe.netty", "org.apache.flink.kinesis.shaded.com.typesafe.netty")
    relocate("org.reactivestream", "org.apache.flink.kinesis.shaded.org.reactivestreams")
}

val testsJar by tasks.registering(Jar::class) {
    archiveClassifier.set("tests")
    from(sourceSets["test"].output)
}

(publishing.publications["maven"] as MavenPublication).artifact(testsJar)
