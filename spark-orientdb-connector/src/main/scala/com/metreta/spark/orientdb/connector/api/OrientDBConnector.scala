
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.api

import com.orientechnologies.orient.core.command.OCommandRequest
import com.orientechnologies.orient.core.config.OGlobalConfiguration
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.orientechnologies.orient.core.sql.query.{OResultSet, OSQLSynchQuery}
import com.tinkerpop.blueprints.impls.orient.{OrientGraph, OrientGraphNoTx}
import com.typesafe.config.Config
import org.apache.spark.{Logging, SparkConf}

/**
 * This is the connection manager, all OrientDB operations must be executed within this class.
 */
class OrientDBConnector(conf: OrientDBConnectorConf)
    extends Serializable with Logging {

  /**
   *  Specifies the storage type of the database to create. It can be one of the supported types:
   *  plocal - persistent database, the storage is written to the file system.
   *  memory - volatile database, the storage is completely in memory.
   *  remote - the storage will be opened via a remote network connection.
   */
  val connProtocol = conf.protocol

  /**
   * Database host.
   */
  val connNodes = conf.nodes

  /**
   * Database name.
   */
  val connDbname = conf.dbName

  /**
   * Database port.
   */
  val connPort = conf.port

  /**
   * Database cluster mode.
   */
  val clusterMode = conf.colocated

  /**
   * Database user name.
   */
  val user = conf.user

  /**
   * Database password.
   */
  val pass = conf.password

  val connStringRemote = connProtocol + ":" + connNodes.head.getHostAddress + ":" + connPort + "/" + connDbname
  val connStringLocalhost = connProtocol + ":" + "localhost:" + connPort + "/" + connDbname
  val connStringLocal = connProtocol + ":/" + connDbname

  def connectDB(connString: String): OPartitionedDatabasePool = {
    logInfo(s"Connection string: $connString")
    new OPartitionedDatabasePool(connString, user, pass)
  }

  def connectRandomDB(): OPartitionedDatabasePool = {
    val uri = connProtocol + ":" + connNodes.toList((math.random * connNodes.size).toInt).getHostAddress + ":" + connPort + "/" + connDbname
    logInfo(s"Connection string: $uri")
    new OPartitionedDatabasePool(uri, user, pass)
  }

  /**
   * Releases a connection from database connection pool.
   * @param pool
   */
  def closePoolConnection(pool: OPartitionedDatabasePool) { pool.close }

  /**
   * Closes an opened database.
   * @param session
   */
  def closeDB(session: ODatabaseDocumentTx) { session.close }

  /**
   * Commits the current transaction.
   * @param session
   */
  def commit(session: ODatabaseDocumentTx) { session.commit }

  /**
   * Commits the current transaction.
   * @param session
   */
  def commit(session: OrientGraph) { session.commit }

  /**
   * Aborts the current running transaction
   * @param session
   */
  def rollback(session: ODatabaseDocumentTx) { session.rollback }

  /**
   * Returns the current status of database.
   * @param session
   */
  def getStatus(session: ODatabaseDocumentTx) { session.getStatus }

  /**
   * Returns OrientDB metadata instance.
   * @param session
   * @return [[com.orientechnologies.orient.core.metadata.OMetadataDefault]]
   */
  def getMetadata(session: ODatabaseDocumentTx) = { session.getMetadata }

  /**
   * Returns OrientDB schema instance.
   * @param session
   * @return [[com.orientechnologies.orient.core.metadata.schema.OSchemaProxy]]
   */
  def getSchema(session: ODatabaseDocumentTx) = { session.getMetadata.getSchema }

  /**
   * Pay attention: OrientDB @Internal
   * Possible signature change or removal
   */
  def getStorage(session: ODatabaseDocumentTx) = { session.getStorage }

  /**
   * Executes a query against the database.
   * @param session
   * @param orientquery
   * @return the results list.
   */
  def query(session: ODatabaseDocumentTx, orientquery: OSQLSynchQuery[_]): OResultSet[Any] = { session.query(orientquery) }

  /**
   * Executes a command against the database. A command can be a SQL statement or a Procedure
   * @param session
   * @param iCommand
   */
  def executeCommand(session: ODatabaseDocumentTx, iCommand: OCommandSQL) = {
    session.command(iCommand).execute()
  }

  /**
   * Executes a command against the database. A command can be a SQL statement or a Procedure
   * @param session
   * @param iCommand
   */
  def executeCommandWithReturn(session: ODatabaseDocumentTx, iCommand: OCommandRequest): Any = {
    session.command(iCommand).execute()

  }

  /**
   * Acquires a connection from a remote connection pool.
   * @return an active database instance.
   */
  def databaseDocumentTx(): ODatabaseDocumentTx = {
     if (clusterMode)
        new ODatabaseDocumentTx(connStringLocalhost).open(user, pass)
     else
      new ODatabaseDocumentTx(connProtocol + ":" + connNodes.toList((math.random * connNodes.size).toInt).getHostAddress + ":" + connPort + "/" + connDbname).open(user, pass)

  }

  /**
   * Creates a new connection to a local database.
   * @return an inactive database instance, it needs to be opened.
   */
  def databaseDocumentTxLocal(): ODatabaseDocumentTx = {
    new ODatabaseDocumentTx(connStringLocal)
  }

  //  def databaseDocumentTxFromPoolLocal(): ODatabaseDocumentTx = {
  //    val pool: OPartitionedDatabasePool = connectDB(connStringLocal)
  //    pool.acquire()
  //  }

  /**
   * Creates a new Transactional Graph using an existent database instance.
   * @return an active graph database instance.
   */
  def databaseGraphTx(): OrientGraph = {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    new OrientGraph(pool().acquire());
  }

  /**
   * Constructs a new object using an existent database instance.
   * @return an active graph database instance.
   */
  def databaseGraphNoTx(): OrientGraphNoTx = {
    val pool = connectDB(connStringRemote)
    new OrientGraphNoTx(pool.acquire());
  }

  /**
   * Creates a new connection to the database.
   * @return an inactive graph database instance, it needs to be opened.
   */
  def databaseGraphTxLocal(): OrientGraph = {
    new OrientGraph(connStringLocal, user, pass);
  }
  /**
   * Creates a new connection pool to the database.
   * @return a connection pool.
   */
  def pool(): OPartitionedDatabasePool = {
    if (clusterMode)
      connectDB(connStringLocalhost)
    else
      connectRandomDB()
  }

}

object OrientDBConnector {

  def apply(conf: SparkConf): OrientDBConnector = {
    new OrientDBConnector(OrientDBConnectorConf(conf))
  }

  def apply(config: Config): OrientDBConnector =
    new OrientDBConnector(OrientDBConnectorConf(config))

}
