import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "spark-assignment-02-solution",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += sparkCore,
    parallelExecution in Test := false
)
