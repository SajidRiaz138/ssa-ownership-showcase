# syntax=docker/dockerfile:1
# Multi-stage: build the shaded jar with Maven+JDK21, run on a slim JRE.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
# copy the whole reactor (parent + modules) so the module can resolve reconciler-core
COPY . .
RUN ./mvnw -q -pl config-reconciler -am -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
# non-root
RUN useradd -r -u 10001 app || true
COPY --from=build /src/config-reconciler/target/config-reconciler-*.jar /app/app.jar
USER 10001
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
