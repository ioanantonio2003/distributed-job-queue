# Build context is the repository root (see docker-compose.yml): the server needs jobqueue-common.

# ---- Stage 1: build with a full JDK ----
FROM eclipse-temurin:21-jdk AS build
WORKDIR /build

COPY .mvn .mvn
COPY mvnw pom.xml ./
COPY jobqueue-common/pom.xml jobqueue-common/pom.xml
COPY jobqueue-server/pom.xml jobqueue-server/pom.xml
COPY jobqueue-worker/pom.xml jobqueue-worker/pom.xml
COPY jobqueue-common/src jobqueue-common/src
COPY jobqueue-server/src jobqueue-server/src

# Windows checkouts may have CRLF line endings, which break the wrapper script and its properties on Linux.
RUN sed -i 's/\r$//' mvnw .mvn/wrapper/maven-wrapper.properties && chmod +x mvnw

# The cache mount keeps downloaded dependencies between builds. Tests are skipped: they need Docker themselves
# (Testcontainers) and run in CI instead. -am also builds jobqueue-common, which the server depends on.
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B -q -pl jobqueue-server -am package -DskipTests

# ---- Stage 2: run with a JRE only (smaller, no compiler or Maven) ----
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Never run as root inside the container.
RUN addgroup -S app && adduser -S app -G app
USER app

COPY --from=build /build/jobqueue-server/target/jobqueue-server-*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
