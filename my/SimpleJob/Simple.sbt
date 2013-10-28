name := "Simple Project"

version := "1.0"

scalaVersion := "2.9.2"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.6.0"

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Spray Repository" at "http://repo.spray.cc/")
