val scala3Version = "3.2.2"
val projectName = "platdb"
val projectVersion = "0.13.0"

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

ThisBuild / organization := "io.github.flxj"
ThisBuild / organizationName := "platdb"
ThisBuild / organizationHomepage := Some(url("https://github.com/flxj/platdb"))
ThisBuild / versionScheme := Some("early-semver")

name := projectName

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/flxj/platdb"),
    "scm:git@github.com:flxj/platdb.git"

  )
)
ThisBuild / developers := List(
  Developer(
    id = "flxj",
    name = "flxj",
    email = "your@email",
    url = url("https://github.com/flxj")
  )
)

ThisBuild / description := "paltdb is a simple key-value storage engine implement by scala."
ThisBuild / licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")
)
ThisBuild / homepage := Some(url("https://github.com/flxj/platdb"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  // For accounts created after Feb 2021:
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true
ThisBuild / publishConfiguration := publishConfiguration.value.withOverwrite(true)
ThisBuild / publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)
//Compile / pushRemoteCacheConfiguration := pushRemoteCacheConfiguration.value.withOverwrite(true)
