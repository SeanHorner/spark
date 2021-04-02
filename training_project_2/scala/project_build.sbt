name := "twitter_testing"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "com.danielasfregola" %% "twitter4s" % "7.0"
resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12"
libraryDependencies += "commons-io" % "commons-io" % "2.8.0"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.2"