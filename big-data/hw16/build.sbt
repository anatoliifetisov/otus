lazy val root = (project in file(".")).
  settings (
    name := "hw16",
    version := "1.0",
    scalaVersion := "2.11.8",
    mainClass in Compile := Some("StackOverflowTagPredictor")
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0" % "provided"
libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "1.4.1"
libraryDependencies += "org.ccil.cowan.tagsoup" % "tagsoup" % "1.2.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}