# --- Stage 1: Builder ---
FROM maven:3.9-eclipse-temurin-21 AS builder

WORKDIR /app

# Copy the Project Object Model (POM) first to cache dependencies
COPY pom.xml .

# Download dependencies (offline mode to speed up subsequent builds)
RUN mvn dependency:go-offline

# Copy the source code
COPY src ./src

# Build the application (Ensure your pom.xml uses maven-shade-plugin to build a fat jar!)
RUN mvn package -DskipTests

# --- Stage 2: Runner ---
# Using 'jammy' (Ubuntu) instead of 'alpine' to ensure glibc compatibility for gRPC/Netty native libraries
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# Copy the jar from the builder stage
COPY --from=builder /app/target/repro-1.0-SNAPSHOT.jar app.jar

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]