FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
# proto/ lives one level up from this module and is referenced by the
# protobuf-maven-plugin's <protoSourceRoot> - copy the whole lab context,
# not just this module, when building this image.
COPY proto ./proto
COPY dispatch-server ./dispatch-server
WORKDIR /build/dispatch-server
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/dispatch-server/target/dispatch-server-0.1.0.jar app.jar
EXPOSE 8400 9090
ENTRYPOINT ["java", "-jar", "app.jar"]
