/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
}

val bundledAkka: Configuration by configurations.creating

dependencies {
    implementation(project(":flink-core"))
    implementation(project(":flink-rpc-core"))
    testImplementation(project(":flink-test-utils-junit"))
    add("bundledAkka", project(":flink-rpc-akka", "shadow"))
}

description = "Flink : RPC : Akka-Loader"

// Bundle the flink-rpc-akka.jar into the output artifact.
val jar = tasks.named<Jar>("jar")
jar.configure {
    dependsOn(bundledAkka)
    from(bundledAkka.singleFile) {
        rename { "flink-rpc-akka.jar" }
    }
}

// Add the output jar to the test classpath.
tasks.named<Test>("test") {
    dependsOn(jar)
    classpath += files(jar.get().archiveFile)
}
