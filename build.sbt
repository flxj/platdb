val scala3Version = "3.2.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "platdb",
    version := "0.10.0",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test
  )

scalacOptions ++= Seq("-encoding", "utf8")
javacOptions ++= Seq("-encoding", "utf8")
Compile / doc / scalacOptions ++= Seq("-siteroot", "docs")
Compile / doc / scalacOptions ++= Seq("-project", "platdb")

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

val AkkaVersion = "2.7.0"
val AkkaHttpVersion = "10.5.2"
libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion
    )