plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("log4j:log4j:1.2.17")
    implementation("org.apache.spark:spark-core_2.13:4.1.0")
    implementation("org.apache.spark:spark-mllib_2.13:4.1.0")
}

tasks.test {
    useJUnitPlatform()
}