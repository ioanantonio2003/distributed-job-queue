# Build context is the repository root (see docker-compose.yml): the worker needs jobqueue-common.

# ---- Stage 1: build with a full JDK ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /build

COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY jobqueue-common/pom.xml jobqueue-common/pom.xml
COPY jobqueue-server/pom.xml jobqueue-server/pom.xml
COPY jobqueue-worker/pom.xml jobqueue-worker/pom.xml
COPY jobqueue-common/src jobqueue-common/src
COPY jobqueue-worker/src jobqueue-worker/src

# Windows checkouts may have CRLF line endings, which break the wrapper script and its properties on Linux.
RUN sed -i 's/\r$//' mvnw .mvn/wrapper/maven-wrapper.properties && chmod +x mvnw

# The cache mount keeps downloaded dependencies between builds. Tests are skipped: they need Docker themselves
# (Testcontainers) and run in CI instead. -am also builds jobqueue-common, which the worker depends on.
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B -q -pl jobqueue-worker -am package -DskipTests

# ---- Stage 2: run with a JRE only (smaller, no compiler or Maven) ----
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Never run as root inside the container.
RUN addgroup -S app && adduser -S app -G app
USER app

COPY --from=build /build/jobqueue-worker/target/jobqueue-worker-*.jar app.jar

# 8081 is the actuator (health) port; it is not published to the host so the service can be scaled.
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
