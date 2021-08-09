name := "spark-datatype-com.awsproserve.validation"

version := "1.0"

scalaVersion := "2.11.8"

assemblyJarName in assembly := "spark-datatype-validation_scala_2.11_assembly.jar"

libraryDependencies ++= Seq(
//  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.4.7" % "provided",
  "com.typesafe.play" %% "play-json" % "2.7.4"
)

// Fix to address incompatible jackson databind module compilation error
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

//noinspection ScalaUnusedSymbol
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
