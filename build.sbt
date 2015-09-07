
 lazy val commonSettings = Seq(
  organization := "com.metreta",
  version := "1.4-alpha-1",
  scalaVersion := "2.11.7",
  libraryDependencies ++= Seq(
    "com.orientechnologies" % "orientdb-core" % "2.1.0",
    "com.orientechnologies" % "orientdb-client" % "2.1.0",
    "com.orientechnologies" % "orientdb-jdbc" % "2.1.0",
    "org.apache.spark" % "spark-core_2.11" % "1.4.0",
    "org.scalatest" % "scalatest_2.11" % "2.2.4",
    "org.apache.spark" % "spark-graphx_2.11" % "1.4.0",
    "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0",
    "com.orientechnologies" % "orientdb-graphdb" % "2.1.0",
    "com.orientechnologies" % "orientdb-distributed" % "2.1.0"
    )
)

lazy val connector = (project in file("./spark-orientdb-connector")).
  settings(commonSettings: _*).
  settings(
    name := "spark-orientdb-connector"
    )
    
lazy val demos = (project in file("./spark-orientdb-connector-demos")).
  settings(commonSettings: _*).
  settings(
    name := "spark-orientdb-connector-demos"
    ).
   dependsOn(connector)
    
	
	
	
 	 	