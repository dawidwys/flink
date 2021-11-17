/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
    id("antlr")
}

dependencies {
    implementation("org.antlr:antlr4-runtime:4.7")
    implementation(project(":flink-core"))
    testImplementation(project(":flink-test-utils-junit"))
    api(project(":flink-streaming-java"))
    add("antlr", "org.antlr:antlr4:4.7.2")
    antlr("org.antlr:antlr4:4.7.1")
}

description = "Flink : Table : Code Splitter"

//sourceSets["main"].antlr.srcDirs(file("${projectDir}/src/main/antlr4"))


tasks.named<AntlrTask>("generateGrammarSource") {
    maxHeapSize = "64m"
    arguments = listOf("-visitor")
    //outputDirectory = file("${buildDir}/generated-sources/antlr4/org/apache/flink/table/codesplit")
}

