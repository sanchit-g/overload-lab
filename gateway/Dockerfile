FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

COPY vendor/distributed-rate-limiter vendor/distributed-rate-limiter
RUN mvn -q -B -f vendor/distributed-rate-limiter/pom.xml \
        -pl rate-limiter-spring-boot-starter -am install -DskipTests

COPY pom.xml .
COPY gateway/pom.xml gateway/
COPY gateway/src gateway/src
RUN mvn -q -B -f gateway/pom.xml package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/gateway/target/gateway-0.1.0.jar app.jar
EXPOSE 8080
# 256MB heap so stage s0 reaches OOM in minutes rather than an hour.
ENV JAVA_OPTS="-Xmx256m -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
