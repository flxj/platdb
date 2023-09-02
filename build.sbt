val scala3Version = "3.2.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "platdb",
    version := "0.9.0-alpha",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test
  )

scalacOptions ++= Seq("-encoding", "utf8")
javacOptions ++= Seq("-encoding", "utf8")