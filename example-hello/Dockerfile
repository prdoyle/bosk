FROM eclipse-temurin:22-jre-alpine

WORKDIR /app
ENTRYPOINT ["java", "-jar", "/app/example-hello.jar"]
EXPOSE 1111

COPY build/libs/example-hello-0.0.1-SNAPSHOT.jar /app/example-hello.jar
