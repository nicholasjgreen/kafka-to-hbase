import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.21"
    application
}

group = "uk.gov.dwp.dataworks"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("org.jetbrains.kotlinx", "kotlinx-coroutines-core", "1.2.2")
    implementation("org.apache.hbase", "hbase-client", "1.4.9")
    implementation("org.apache.kafka", "kafka-clients", "2.3.0")
    testImplementation("io.kotlintest", "kotlintest-runner-junit5", "3.3.2")
}

tasks.withType<Test> {
    useJUnitPlatform { }
}

application {
    mainClassName = "Kafka2HbaseKt"
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

sourceSets {
    create("integration") {
        java.srcDir(file("src/integration/groovy"))
        java.srcDir(file("src/integration/kotlin"))
        compileClasspath += sourceSets.getByName("main").output + configurations.testRuntimeClasspath
        runtimeClasspath += output + compileClasspath
    }
}

tasks.register<Test>("integration") {
    description = "Runs the integration tests."
    group = "verification"
    testClassesDirs = sourceSets["integration"].output.classesDirs
    classpath = sourceSets["integration"].runtimeClasspath

    useJUnitPlatform { }
    testLogging {
        exceptionFormat = TestExceptionFormat.FULL
        events = setOf(TestLogEvent.SKIPPED, TestLogEvent.PASSED, TestLogEvent.FAILED)
    }
}