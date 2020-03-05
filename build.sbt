name := "project-1"

version := "0.1"

scalaVersion := "2.13.1"

val calciteVersion = "1.21.0"

initialize := {
  val _ = initialize.value // run the previous initialization
  val required = VersionNumber("10")
  val current  = VersionNumber(sys.props("java.specification.version"))
  assert(current.matchesSemVer(SemanticSelector(">=10")), s"Unsupported JDK: java.specification.version $current < $required")
}

// https://mvnrepository.com/artifact/org.apache.calcite/calcite-core
// Include Calcite Core
libraryDependencies += "org.apache.calcite" % "calcite-core" % calciteVersion
// Also include the tests.jar of Calcite Core as a dependency to our testing jar
libraryDependencies += "org.apache.calcite" % "calcite-core" % calciteVersion % Test classifier "tests"
libraryDependencies += "org.apache.calcite" % "calcite-example-csv" % calciteVersion

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

// Calcite DDL Parser
// https://mvnrepository.com/artifact/org.apache.calcite/calcite-server
libraryDependencies += "org.apache.calcite" % "calcite-server" % calciteVersion

// https://mvnrepository.com/artifact/org.json4s/json4s-jackson_2.10
libraryDependencies += "org.json4s" % "json4s-jackson_2.12" % "3.5.3"

// slf4j
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.13"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.13"

// https://mvnrepository.com/artifact/org.apache.calcite.avatica/avatica
libraryDependencies += "org.apache.calcite.avatica" % "avatica-server" % "1.15.0"
// https://mvnrepository.com/artifact/com.google.guava/guava
libraryDependencies += "com.google.guava" % "guava" % "19.0"
// https://mvnrepository.com/artifact/au.com.bytecode/opencsv
libraryDependencies += "au.com.bytecode" % "opencsv" % "2.4"
// https://mvnrepository.com/artifact/commons-io/commons-io
libraryDependencies += "commons-io" % "commons-io" % "2.4"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.4"

// https://mvnrepository.com/artifact/org.junit.jupiter/junit-jupiter-api
libraryDependencies += "org.junit.jupiter" % "junit-jupiter-api" % "5.3.1" % Test
libraryDependencies += "org.junit.jupiter" % "junit-jupiter-params" % "5.3.1" % Test

// junit tests (invoke with `sbt test`)
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test"

fork in Test := true

// disable tests during `sbt assembly`
test in assembly := {}

resolvers += Resolver.jcenterRepo
testOptions += Tests.Argument(jupiterTestFramework, "-q")

libraryDependencies += "net.aichler" % "jupiter-interface" % JupiterKeys.jupiterVersion.value % Test

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", _ @ _*) => MergeStrategy.first
  case PathList("META-INF", _ @ _*) => MergeStrategy.discard
}

