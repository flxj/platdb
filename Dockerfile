FROM openjdk:17-jdk-alpine
EXPOSE 8080

WORKDIR /app
COPY ./target/scala-3.2.2/platdb-0.12.0-SNAPSHOT.jar /app
COPY ./example/platdb.conf /app

CMD ["java", "-jar", "platdb-0.12.0-SNAPSHOT.jar","platdb.conf"]
