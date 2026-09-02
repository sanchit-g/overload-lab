FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
COPY downstream-sim/pom.xml downstream-sim/
COPY downstream-sim/src downstream-sim/src
RUN mvn -q -B -f downstream-sim/pom.xml package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/downstream-sim/target/downstream-sim-0.1.0.jar app.jar
EXPOSE 9090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
