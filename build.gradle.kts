import org.gradle.api.tasks.Exec

plugins {
    id("java")
    id("nebula.release") version("19.0.10")
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
}

tasks.test {
    useJUnitPlatform()
}

tasks.register("pushToGithub") {
    doLast {
        exec {
            commandLine("git", "commit", "-am", "Version $version")
        }
        exec {
            commandLine("git", "push", "-u", "origin")
        }
    }
}
