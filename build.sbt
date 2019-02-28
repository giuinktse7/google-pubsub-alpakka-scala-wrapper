name := "school"

version := "0.1"

scalaVersion := "2.12.3"

mainClass in (Compile, run) := Some("com.Main")

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "com.pauldijou" %% "jwt-core" % "0.16.0",
  "com.lightbend.akka" %% "akka-stream-alpakka-google-cloud-pub-sub" % "1.0-M2",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.7",
  "com.typesafe.akka" %% "akka-http" % "10.1.7",
  "com.typesafe.akka" %% "akka-stream" % "2.5.19",
  "com.typesafe" % "config" % "1.3.2",
  "com.google.cloud" % "google-cloud-pubsub" % "1.44.0"
)