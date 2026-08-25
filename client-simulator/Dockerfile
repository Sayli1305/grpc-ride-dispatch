FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY proto ./proto
COPY client-simulator ./client-simulator
WORKDIR /build/client-simulator
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/client-simulator/target/client-simulator-0.1.0.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
