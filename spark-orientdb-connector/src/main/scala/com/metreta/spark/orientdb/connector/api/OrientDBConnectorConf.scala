
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.api

import java.net.InetAddress

import com.typesafe.config.Config
import org.apache.spark.{ Logging, SparkConf }

import scala.util.control.NonFatal

/**
  * @author Simone
  */
case class OrientDBConnectorConf(
  nodes: Set[InetAddress],
  port: String,
  protocol: String,
  dbName: String,
  user: String,
  password: String,
  colocated: Boolean)

object OrientDBConnectorConf extends Logging {

  val OrientDBNodesProperty = "spark.orientdb.connection.nodes"
  //  val DefaultOrientDBNodesProperty = "127.0.0.1"

  val OriendtDBProtocolProperty = "spark.orientdb.protocol"
  val DefaultOriendtDBProtocolProperty = "plocal"

  val OriendtDBDBNameProperty = "spark.orientdb.dbname"
  val DefaultOriendtDBDBNameProperty = ""

  val OriendtDBPortProperty = "spark.orientdb.port"
  val DefaultOriendtDBPortProperty = "2424"

  val OriendtDBUserProperty = "spark.orientdb.user"

  val OriendtDBPasswordProperty = "spark.orientdb.password"

  val OriendtDBClusterModeProperty = "spark.orientdb.clustermode" //remote-colocated
  val DefaultOriendtDBClusterModeProperty = false

  def apply(sparkConf: SparkConf): OrientDBConnectorConf = {

    val nodesProperty = sparkConf.get(OrientDBNodesProperty, InetAddress.getLocalHost.getHostAddress)
    val nodes = for {
      nodeName ← nodesProperty.split(",").toSet[String]
      nodeAddress ← getHost(nodeName)
    } yield nodeAddress

    val protocol = sparkConf.get(OriendtDBProtocolProperty, DefaultOriendtDBProtocolProperty)
    val dbname = sparkConf.get(OriendtDBDBNameProperty, DefaultOriendtDBDBNameProperty)
    val port = sparkConf.get(OriendtDBPortProperty, DefaultOriendtDBPortProperty)
    val user = sparkConf.get(OriendtDBUserProperty)
    val password = sparkConf.get(OriendtDBPasswordProperty)
    val clusterMode = sparkConf.get(OriendtDBClusterModeProperty).equals("colocated")

    OrientDBConnectorConf(nodes, port, protocol, dbname, user, password, clusterMode)
  }

  def apply(config: Config): OrientDBConnectorConf = {

    def getStringOrElse(path: String, default: String): String =
      if (config.hasPath(path))
        config.getString(path)
      else
        default

    val nodesProperty = getStringOrElse(OrientDBNodesProperty, InetAddress.getLocalHost.getHostAddress)

    val nodes = for {
      nodeName ← nodesProperty.split(",").toSet[String]
      nodeAddress ← getHost(nodeName)
    } yield nodeAddress

    val protocol = getStringOrElse(OriendtDBProtocolProperty, DefaultOriendtDBProtocolProperty)
    val dbname = getStringOrElse(OriendtDBDBNameProperty, DefaultOriendtDBDBNameProperty)
    val port = getStringOrElse(OriendtDBPortProperty, DefaultOriendtDBPortProperty)
    val user = getStringOrElse(OriendtDBUserProperty, "")
    val password = config.getString(OriendtDBPasswordProperty)
    val clusterMode = config.getString(OriendtDBClusterModeProperty).equals("colocated")

    OrientDBConnectorConf(nodes, port, protocol, dbname, user, password, clusterMode)
  }

  private def getHost(hostName: String): Option[InetAddress] = {
    try {
      val host = InetAddress.getByName(hostName)
      Some(host)
    } catch {
      case NonFatal(e) ⇒
        logError("Host " + hostName + " not found", e)
        None
    }
  }
}
