plugins {
    id("java")
}

group = "com.us.unix.cbclone.couchbase2"
version = "1.0.0"

repositories {
    mavenCentral()
    maven {
        url = uri("https://repo1.maven.org/maven2")
    }
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("com.couchbase.client:java-client:2.7.23")
    implementation("com.couchbase.client:dcp-client:0.31.0")
    implementation("org.apache.logging.log4j:log4j-core:2.24.0")
    implementation("org.apache.logging.log4j:log4j-api:2.24.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.24.0")
    implementation("com.fasterxml.jackson.core:jackson-core:2.17.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.codelry.util:restfull-core")
    implementation(project(":core"))
}

tasks.test {
    useJUnitPlatform()
    exclude("**/*")
}
