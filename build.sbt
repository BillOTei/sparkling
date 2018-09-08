name := "sparkling"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.3.1"
val spName = "sparkling"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion + "_" + module.revision + "." + artifact.extension
}