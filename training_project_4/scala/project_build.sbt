name := "BD-Project-Analysis"

version := "0.1"

scalaVersion := "2.12.10"

resolvers += Resolver.bintrayRepo("cibotech", "public")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

libraryDependencies += "org.scalaj" % "scalaj-http_2.12" % "2.4.2"

libraryDependencies += "net.liftweb" %% "lift-json" % "3.4.2"

// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12"

libraryDependencies += "io.spray" %%  "spray-json" % "1.3.6"

libraryDependencies += "com.cibo" %% "evilplot" % "0.6.3"