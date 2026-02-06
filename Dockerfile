# ---------- Build stage ----------
FROM registry.redhat.io/ubi8/openjdk-8 AS build

WORKDIR /opt/app-root/src

# UBI OpenJDK image usually doesn't include Maven + ensure WORKDIR is writable
USER root
RUN microdnf install -y maven && microdnf clean all \
 && mkdir -p /opt/app-root/src \
 && chgrp -R 0 /opt/app-root/src \
 && chmod -R g=u /opt/app-root/src

# Switch back to non-root (OpenShift compatible)
USER 1001

# Copy project files
COPY pom.xml .
COPY src ./src

# Build (fat jar if your pom is configured for it)
RUN mvn -B -DskipTests clean package

# ---------- Runtime stage ----------
FROM registry.redhat.io/ubi8/openjdk-8-runtime

WORKDIR /app
COPY --from=build /opt/app-root/src/target/*.jar /app/app.jar

# OpenShift arbitrary UID support
RUN chgrp -R 0 /app && chmod -R g=u /app

ENV JAVA_TOOL_OPTIONS="\
  -XX:MaxRAMPercentage=75 \
  -Djava.security.egd=file:/dev/urandom \
"

ENTRYPOINT ["java","-jar","/app/app.jar"]
