package com.metreta.spark.orientdb.connector.demo

import org.apache.spark.{ Logging, SparkContext, SparkConf }
import com.metreta.spark.orientdb.connector.SparkContextFunctions

trait DemoUtils extends Logging {

  val OrientDBNodesProperty = "spark.orientdb.connection.nodes"
  val DefaultOrientDBNodesProperty = "127.0.0.1"

  val OriendtDBProtocolProperty = "spark.orientdb.protocol"
  val DefaultOriendtDBProtocolProperty = "plocal"

  val OriendtDBDBNameProperty = "spark.orientdb.dbname"
  //  val DefaultOriendtDBDBNameProperty = "testdb"
  val DefaultOriendtDBDBNameProperty = """/path/to/orient"""

  val OriendtDBPortProperty = "spark.orientdb.port"
  val DefaultOriendtDBPortProperty = "2424"

  val OriendtDBUserProperty = "spark.orientdb.user"
  val DefaultOriendtDBUser = "admin"

  val OriendtDBPasswordProperty = "spark.orientdb.password"
  val DefaultOriendtDBPassword = "admin"

  val OriendtDBClusterModeProperty = "spark.orientdb.clustermode" //remote-colocated
  val DefaultOriendtDBClusterMode = "colocated"

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions = new SparkContextFunctions(sc)

  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("demo")
    .set(OrientDBNodesProperty, DefaultOrientDBNodesProperty)
    .set(OriendtDBProtocolProperty, DefaultOriendtDBProtocolProperty)
    .set(OriendtDBDBNameProperty, DefaultOriendtDBDBNameProperty)
    .set(OriendtDBPortProperty, DefaultOriendtDBPortProperty)
    .set(OriendtDBUserProperty, DefaultOriendtDBUser)
    .set(OriendtDBPasswordProperty, DefaultOriendtDBPassword)
    .set(OriendtDBClusterModeProperty, DefaultOriendtDBClusterMode)

  lazy val sc = new SparkContext(conf)

}

object DemoUtils {
  def apply(): DemoUtils = new DemoUtils {}
}


