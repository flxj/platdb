FROM sbtscala/scala-sbt:eclipse-temurin-21_1.10.0_3.3.1 AS builder

WORKDIR /app
COPY build.sbt ./
COPY project/plugins.sbt project/build.properties ./project/

RUN sbt update

COPY src ./src
RUN sbt assembly

FROM openjdk:17-jdk-alpine
EXPOSE 8080

WORKDIR /app
COPY --from=builder /app/target/scala-3.2.2/platdb-0.15.1-SNAPSHOT.jar /app
COPY ./example/platdb.conf /app

CMD ["java", "-jar", "platdb-0.15.1-SNAPSHOT.jar","platdb.conf"]
