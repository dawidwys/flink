/*
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("org.apache.flink.java-conventions")
    id("scala")
}

dependencies {
    implementation(project(":flink-table-api-scala_2.12"))
    implementation(project(":flink-scala_2.12"))
    implementation(project(":flink-streaming-scala_2.12"))
    testImplementation(project(":flink-table-common"))
    testImplementation(project(":flink-table-api-java"))
}

description = "Flink : Table : API Scala bridge"

sourceSets {
    named("main") {
        withConvention(ScalaSourceSet::class) {
            scala {
                setSrcDirs(listOf("src/main/scala", "src/main/java"))
            }
        }
        java {
            setSrcDirs(emptyList<String>())
        }
    }
    named("test") {
        withConvention(ScalaSourceSet::class) {
            scala {
                setSrcDirs(listOf("src/test/scala", "src/test/java"))
            }
        }
        java {
            setSrcDirs(emptyList<String>())
        }
    }
}
