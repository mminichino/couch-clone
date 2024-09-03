import java.io.ByteArrayOutputStream

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
}

tasks.test {
    useJUnitPlatform()
}

tasks.register("pushToGithub") {
    val stdout = ByteArrayOutputStream()
    doLast {
        exec {
            commandLine("git", "commit", "-am", "Version $version")
            standardOutput = stdout
        }
        exec {
            commandLine("git", "push", "-u", "origin")
            standardOutput = stdout
        }
        println(stdout)
    }
}
