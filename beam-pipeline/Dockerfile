FROM gradle:8.7-jdk17 AS build
WORKDIR /workspace
COPY . /workspace
RUN gradle --no-daemon shadowJar

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/build/libs/app-all.jar /app/app-all.jar
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
