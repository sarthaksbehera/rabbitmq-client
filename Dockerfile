# ---------- Build stage ----------
FROM registry.redhat.io/ubi8/openjdk-8 AS build
WORKDIR /opt/app-root/src
# Copy pom first for caching
COPY pom.xml ./
COPY src ./src
# Build fat jar
RUN mvn -B -DskipTests package
# ---------- Runtime stage ----------
FROM registry.redhat.io/ubi8/openjdk-8-runtime
WORKDIR /app
COPY --from=build /opt/app-root/src/target/*jar /app/app.jar
# OpenShift arbitrary UID support
RUN chgrp -R 0 /app && chmod -R g=u /app
ENV JAVA_TOOL_OPTIONS="\
 -XX:+UnlockExperimentalVMOptions \
 -XX:+UseCGroupMemoryLimitForHeap \
 -XX:MaxRAMPercentage=75 \
 -Djava.security.egd=file:/dev/urandom \
"
ENTRYPOINT ["java","-jar","/app/app.jar"]
