lazy val root = project.in(file(".")).aggregate(streaming, analysis, aggregation, web)

def commonSettings = Seq(
  scalaVersion := "2.11.8",
  version := "1.0",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case PathList("reference.conf") => MergeStrategy.concat
    case x => MergeStrategy.last
  }
)

lazy val common =
  (project in file("common"))
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "io.spray" %% "spray-json" % "1.3.3",
        "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
      )
    )

lazy val configuration =
  (project in file("configuration"))
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.github.pureconfig" %% "pureconfig" % "0.9.1",
        "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0",
        "org.twitter4j" % "twitter4j-core" % "4.0.6"
      )
    )

lazy val streaming =
  (project in file("streaming"))
    .dependsOn(common, configuration)
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
        "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0",
        "org.apache.spark" %% "spark-hive" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0",
        "io.spray" %% "spray-json" % "1.3.3"
      ),
      dependencyOverrides ++= Seq(
        "org.twitter4j" % "twitter4j-core" % "4.0.6", // bahir uses an outdated version of twitter4j
        "org.twitter4j" % "twitter4j-stream" % "4.0.6"
      )
    )

lazy val analysis =
  (project in file("analysis"))
    .dependsOn(common, configuration)
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-hive" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0",
        "io.spray" %% "spray-json" % "1.3.3",
        "edu.stanford.nlp" % "stanford-corenlp" % "3.9.1"
      )
    )

lazy val aggregation =
  (project in file("aggregation"))
    .dependsOn(common, configuration)
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-hive" % "2.2.0" % "provided",
        "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0",
        "io.spray" %% "spray-json" % "1.3.3"
      )
    )

lazy val web =
  (project in file("web"))
    .dependsOn(common, configuration)
    .settings(commonSettings: _*)
    .settings(
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-http" % "10.0.11",
        "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.11",
        "io.spray" %% "spray-json" % "1.3.3"
      )
    )