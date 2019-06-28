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
    implementation("org.apache.hbase", "hbase-client", "1.4.9")
    implementation("org.apache.kafka", "kafka-clients", "2.3.0")
}

application {
    mainClassName = "Kafka2HbaseKt"
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
