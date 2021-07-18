name := "Meetup Data Analysis Tool"

version := "0.1"

scalaVersion := "2.12.10"

resolvers += Resolver.bintrayRepo("cibotech", "public")

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "com.cibo" %% "evilplot" % "0.6.3"