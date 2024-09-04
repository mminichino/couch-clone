import java.util.*

plugins {
    id("java")
}

group = "com.us.unix.cbclone"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("commons-cli:commons-cli:1.9.0")
    implementation("commons-io:commons-io:2.16.1")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.slf4j:slf4j-log4j12:2.0.16")
    implementation("com.fasterxml.jackson.core:jackson-core:2.17.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.squareup.okhttp3:logging-interceptor:4.12.0")
}

tasks.test {
    useJUnitPlatform()
}

tasks.register("createProperties") {
    dependsOn("processResources")

    doLast {
        val propertiesFile = File("${layout.projectDirectory}/src/main/resources/version.properties")
        propertiesFile.writer().use { writer ->
            val properties = Properties().apply {
                setProperty("version", project.version.toString())
            }
            properties.store(writer, null)
        }
    }
}

tasks.named("classes") {
    dependsOn("createProperties")
}
