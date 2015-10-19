
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.api

import java.net.InetAddress
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

  def apply(conf: SparkConf): OrientDBConnectorConf = {
    
    val nodesProperty = conf.get(OrientDBNodesProperty, InetAddress.getLocalHost.getHostAddress)
    val nodes = for {
      nodeName <- nodesProperty.split(",").toSet[String]
      nodeAddress <- getHost(nodeName)
    } yield nodeAddress
    
    val protocol = conf.get(OriendtDBProtocolProperty, DefaultOriendtDBProtocolProperty)
    val dbname = conf.get(OriendtDBDBNameProperty, DefaultOriendtDBDBNameProperty)
    val port = conf.get(OriendtDBPortProperty, DefaultOriendtDBPortProperty)
    val user = conf.get(OriendtDBUserProperty)
    val password = conf.get(OriendtDBPasswordProperty)
    val clusterMode = if(conf.get(OriendtDBClusterModeProperty).equals("colocated")){true}else{false}

    OrientDBConnectorConf(nodes, port, protocol, dbname, user, password, clusterMode)
  }

  private def getHost(hostName: String): Option[InetAddress] = {
    try {
      val host = InetAddress.getByName(hostName)
      Some(host)
    } catch {
      case NonFatal(e) =>
        logError("Host " + hostName + " not found", e)
        None
    }
  }
}
