
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.rdd

import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.metreta.spark.orientdb.connector.rdd.partitioner.{ ClassRDDPartitioner, OrientPartition }
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.{ OResultSet, OSQLSynchQuery }
import org.apache.spark.{ SparkContext, _ }

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

case class OrientDocumentException(message: String) extends Exception(message)
case class OrientDocument(
  oClassName: String,
  oRid: String,
  oColumnNames: IndexedSeq[String],
  oColumnValues: IndexedSeq[Any]) extends OrientEntry(oClassName, oRid, oColumnNames, oColumnValues)

object OrientDocument {
  def fromODocument(doc: ODocument): OrientDocument =
    OrientDocument(
      doc.getClassName,
      doc.getIdentity.toString(),
      doc.fieldNames().toIndexedSeq,
      serialize(doc.fieldValues()).toIndexedSeq)

  private def serialize(fieldValues: Any): Array[Any] =
    fieldValues.asInstanceOf[Array[Any]] map {
      case z: ORidBag ⇒ z.toString()
      case z          ⇒ z
    }
}
/**
  * @author Simone Bronzin
  *
  */
class OrientClassRDD[T] private[connector] (@transient val sc: SparkContext,
  val connector: OrientDBConnector,
  val from: String,
  val where: String = "",
  val depth: Option[Int] = None,
  val query: String = "")(implicit val classTag: ClassTag[T] //,
  //val connector: OrientDBConnector = OrientDBConnector(sc.getConf)
  )
    extends OrientRDD[OrientDocument](sc, Seq.empty) {

  /**
    * Fetches the data from the given partition.
    * @param split
    * @param context
    * @return a ClassRDD[OrientDocument]
    */
  override def compute(split: Partition, context: TaskContext): Iterator[OrientDocument] = {

    //(List(OrientDocument("1", "1", null, null), OrientDocument("2", "2", null, null), OrientDocument("3", "3", null, null))).toIterator

    val session = connector.databaseDocumentTx()

    val partition = split.asInstanceOf[OrientPartition]

    val cluster = partition.partitionName.clusterName

    val queryString = createQueryString(cluster, where, depth) match {
      case Some(i) ⇒ i
      case None ⇒
        throw OrientDocumentException("wrong number of parameters")
        "error"
    }
    val query = new OSQLSynchQuery(queryString)

    val res: OResultSet[Any] = connector.query(session, query)
    logInfo(s"Fetching data from: $cluster")

    res.map {
      case recordId: ORecordId ⇒
        val doc = recordId.getRecord.asInstanceOf[ODocument]
        OrientDocument.fromODocument(doc)
      case doc: ODocument ⇒
        OrientDocument.fromODocument(doc)
    }.iterator
  }

  /**
    * Builds a query string.
    * @param cluster
    * @param where
    * @param depth
    * @return OrientDB query string.
    */
  def createQueryString(cluster: String, where: String, depth: Option[Int]): Option[String] = {
    if (where == "" && depth.isEmpty) {
      Option("select from cluster:" + cluster)
    } else if (where != "" && depth.isEmpty) {
      Option("select from cluster:" + cluster + " where " + where)
    } else if (where == "" && depth.isDefined) {
      Option("traverse * from cluster:" + cluster + " while $depth < " + depth.get)
    } else {
      None
    }
  }
  /**
    * @return Spark partitions from a given OrientdDB class.
    */
  override def getPartitions: Array[Partition] = {

    //    List(OrientPartition(
    //        1,
    //        null, // <- Host Address ?????
    //        PartitionName("writes", "writes_node1"))).toArray
    val partitioner = new ClassRDDPartitioner(connector, from)
    val partitions = partitioner.getPartitions()

    logInfo(s"Found ${partitions.length} clusters.")

    partitions
  }

  // TODO: OrientPartition ha il campo endpoints a null.
  //       come troviamo gli host?

  //  override def getPreferredLocations(split: Partition): Seq[String] = {
  //
  //    println("connector.connStringRemote: " + connector.connStringRemote)
  //    val path = connector.connStringRemote.substring(connector.connStringRemote.indexOf(":") + 1)
  //    println("path: " + path)
  //
  //    println(connector.databaseDocumentTx().getURL)
  //    var server = OServerMain.server()
  //
  //    server.serverLogin(connector.user, connector.pass, connector.connDbname)
  //
  //
  ////    var server: OServer = OServer.getInstanceByPath(path)
  //    println("server: " + server)
  //
  //    val dManager: OHazelcastPlugin = server.getDistributedManager().asInstanceOf[OHazelcastPlugin] //npe
  //    println("connector.connDbname: " + connector.connDbname)
  //    val dbCfg: ODistributedConfiguration = dManager.getDatabaseConfiguration(connector.connDbname);
  //    println("split.asInstanceOf[OrientPartition].partitionName.clusterName: " + split.asInstanceOf[OrientPartition].partitionName.clusterName)
  //    val clusters = Array(split.asInstanceOf[OrientPartition].partitionName.clusterName)
  //    val nodes = dbCfg.getServers(clusters.toSeq);
  //
  //    println("nodes: " + nodes)
  //    nodes.toSeq
  //    null

  //  }

  object OrientClassRDD {

    //        def apply[T : ClassTag ](
    //        sc: SparkContext,
    //        from: String,
    //        where: String,
    //        depth: Option[Int],
    //        query: String
    //        ): OrientClassRDD[T] = {
    //
    //        new OrientClassRDD[T](
    //           sc = sc,
    //          connector = OrientDBConnector(sc.getConf),
    //          from = from,
    //          where = where,
    //         depth = depth,
    //          query: String)
    //      }

    //        def apply[K, V](
    //          sc: SparkContext,
    //          query: String)(
    //        implicit
    //          keyCT: ClassTag[K],
    //          valueCT: ClassTag[V])
    //          : ClassRDD[(K, V)] = {
    //
    //        new ClassRDD[(K, V)](
    //          sc = sc,
    //          connector = OrientDBConnector(sc.getConf),
    //          query: String
    //          )
    //      }

  }
}
