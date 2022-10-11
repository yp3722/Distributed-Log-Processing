ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

lazy val root = (project in file("."))
  .settings(
    name := "StatsForLogs",
    //idePackagePrefix := Some("edu.uic.cs441")
  )

val scalacticVersion = "3.2.9"
val logbackVersion = "1.3.0-alpha10"
val sfl4sVersion = "2.0.0-alpha5"
val typesafeConfigVersion = "1.4.1"
val apacheCommonIOVersion = "2.11.0"
val hadoopVersion = "3.3.4"


//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}

resolvers += Resolver.jcenterRepo


// https://mvnrepository.com/artifact/ch.qos.logback/logback-core b
libraryDependencies += "ch.qos.logback" % "logback-core" % logbackVersion
// https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
libraryDependencies += "ch.qos.logback" % "logback-classic" % logbackVersion % Test
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies += "org.slf4j" % "slf4j-api" % sfl4sVersion

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion

libraryDependencies += "org.scalactic" %% "scalactic" % scalacticVersion
libraryDependencies += "org.scalatest" %% "scalatest" % scalacticVersion % Test
libraryDependencies += "org.scalatest" %% "scalatest-featurespec" % scalacticVersion % Test

libraryDependencies += "com.typesafe" % "config" % typesafeConfigVersion