# ---------- Build stage ----------
FROM registry.redhat.io/ubi8/openjdk-8 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -e -C dependency:go-offline
COPY src ./src
RUN mvn -B package -DskipTests
# ---------- Runtime stage ----------
FROM registry.redhat.io/ubi8/openjdk-8-runtime
WORKDIR /app
COPY --from=build /build/target/*jar /app/app.jar
RUN chgrp -R 0 /app && chmod -R g=u /app
ENV JAVA_TOOL_OPTIONS="\
 -XX:+UnlockExperimentalVMOptions \
 -XX:+UseCGroupMemoryLimitForHeap \
 -XX:MaxRAMPercentage=75 \
 -Djava.security.egd=file:/dev/urandom \
"
ENTRYPOINT ["java","-jar","/app/app.jar"]
