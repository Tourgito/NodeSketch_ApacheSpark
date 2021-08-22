val scala3Version = "2.12.8"

lazy val root = project
  .in(file("."))
  .settings(
    name := "NodeSketch",
    version := "0.1.0",

    scalaVersion := scala3Version,

    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
  )
  
