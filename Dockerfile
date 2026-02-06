# ---------- Build stage ----------
FROM registry.redhat.io/ubi8/openjdk-8 AS build
WORKDIR /build
# Copy Maven wrapper or use system Maven if your image includes it
# If you don't have mvnw, see note below
COPY pom.xml .
COPY src ./src
# Build fat jar (skip tests in CI/container builds)
RUN mvn package -DskipTests
# ---------- Runtime stage ----------
FROM registry.redhat.io/ubi8/openjdk-8-runtime
WORKDIR /app
# Copy built jar
COPY --from=build /build/target/*jar /app/app.jar
# OpenShift: allow arbitrary UID
RUN chgrp -R 0 /app && chmod -R g=u /app
# JVM tuning for containers
ENV JAVA_TOOL_OPTIONS="\
 -XX:+UnlockExperimentalVMOptions \
 -XX:+UseCGroupMemoryLimitForHeap \
 -XX:MaxRAMPercentage=75 \
 -Djava.security.egd=file:/dev/urandom \
"
ENTRYPOINT ["java","-jar","/app/app.jar"]
