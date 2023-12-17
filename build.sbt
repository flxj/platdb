val scala3Version = "3.2.2"
val projectName = "platdb"
val projectVersion = "0.12.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := projectName,
    version := projectVersion,
    scalaVersion := scala3Version,
    assembly / assemblyJarName := s"${projectName}-${projectVersion}.jar",

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test
  )

scalacOptions ++= Seq("-encoding", "utf8")
javacOptions ++= Seq("-encoding", "utf8")
Compile / doc / scalacOptions ++= Seq("-siteroot", "docs")
Compile / doc / scalacOptions ++= Seq("-project", "platdb")

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

val AkkaVersion = "2.8.3"
val AkkaHttpVersion = "10.5.0"
libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion
    )

libraryDependencies ++=Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
      "ch.qos.logback" % "logback-classic" % "1.2.10",
      "com.typesafe" % "config" % "1.4.3"
)
