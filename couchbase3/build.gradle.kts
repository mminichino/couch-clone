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
    implementation("com.couchbase.client:java-client:3.7.2")
    implementation("com.couchbase.client:dcp-client:0.51.0")
    implementation("org.apache.logging.log4j:log4j-core:2.24.0")
    implementation("org.apache.logging.log4j:log4j-api:2.24.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.24.0")
    implementation("com.fasterxml.jackson.core:jackson-core:2.17.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.codelry.util:restfull-core:1.0.1")
    implementation(project(":core"))
}

tasks.test {
    useJUnitPlatform()
    exclude("**/*")
}
