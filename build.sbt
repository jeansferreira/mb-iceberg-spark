enablePlugins(DockerPlugin)

ThisBuild / version := "0.1.56"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "mb-spark-iceberg"
  )

val icebergVersion = "1.1.0"
val sparkVersion = "3.2.1"

libraryDependencies += "org.apache.iceberg" % "iceberg-core" % icebergVersion
libraryDependencies += "org.apache.iceberg" % "iceberg-api" % icebergVersion
libraryDependencies += "org.apache.iceberg" % "iceberg-common" % icebergVersion
libraryDependencies += "org.apache.iceberg" % "iceberg-spark" % icebergVersion
libraryDependencies += "org.apache.iceberg" %% "iceberg-spark-runtime-3.2" % icebergVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.1.0"

ThisBuild / assemblyMergeStrategy := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", "MANIFEST.MF")                => MergeStrategy.discard
  case PathList(xs @ _*)                                  => MergeStrategy.first
  case "application.conf"                                 => MergeStrategy.concat
  case "unwanted.txt"                                     => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}

// Sets a name with a tag that contains the project version
docker / imageNames := Seq(
  ImageName(
    namespace = Some("aqrl-repo-"),
    repository = "mb/spark-iceberg",
    tag = Some("v" + version.value)
  )
)

docker / dockerfile := {

  // The assembly task generates a fat JAR file
  val artifact: File = assembly.value
  val artifactTargetPath = s"/opt/spark/examples/jars/${artifact.name}"

  new Dockerfile {
    // Base image
    from("apache/spark:v3.2.1")
    // Add AWS Credentials
    env(
      "VAR_1" -> "teste1",
      "VAR_2" -> "teste2",
      "VAR_3" -> "teste3"
    )
    // Add the JAR file
    add(artifact, artifactTargetPath)
    user("root")
  }

}