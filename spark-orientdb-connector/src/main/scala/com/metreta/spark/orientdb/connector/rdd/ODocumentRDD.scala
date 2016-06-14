
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.rdd

import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.metreta.spark.orientdb.connector.rdd.partitioner.{ ClassRDDPartitioner, OrientPartition }
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.{ OResultSet, OSQLSynchQuery }
import org.apache.spark.{ SparkContext, _ }

import scala.collection.JavaConversions._

class ODocumentRDD private[connector] (@transient val sc: SparkContext,
  val connector: OrientDBConnector,
  val from: String,
  val what: String = "",
  val where: String = "",
  val depth: Option[Int] = None,
  val query: String = "")
    extends OrientRDD[ODocument](sc, Seq.empty) {

  /**
    * Fetches the data from the given partition.
    * @param split
    * @param context
    * @return a ClassRDD[OrientDocument]
    */
  override def compute(split: Partition, context: TaskContext): Iterator[ODocument] = {

    val session = connector.databaseDocumentTx()

    val partition = split.asInstanceOf[OrientPartition]

    val cluster = partition.partitionName.clusterName

    val queryString = createQueryString(cluster, what, where, depth) match {
      case Some(i) ⇒ i
      case None ⇒
        throw OrientDocumentException("wrong number of parameters")
        "error"
    }
    val query = new OSQLSynchQuery(queryString)

    val res: OResultSet[Any] = connector.query(session, query)
    logInfo(s"Fetching data from: $cluster")

    val res2 = res.map {
      case recordId: ORecordId ⇒ recordId.getRecord.asInstanceOf[ODocument]
      case doc: ODocument      ⇒ doc
    }
    res2.iterator
  }

  /**
    * Builds a query string.
    * @param cluster
    * @param where
    * @param depth
    * @return OrientDB query string.
    */
  def createQueryString(cluster: String, what: String, where: String, depth: Option[Int]): Option[String] = {
    if (where == "" && depth.isEmpty) {
      Option(s"select $what from cluster:" + cluster)
    } else if (where != "" && depth.isEmpty) {
      Option(s"select $what from cluster:" + cluster + " where " + where)
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
    val partitioner = new ClassRDDPartitioner(connector, from)
    val partitions = partitioner.getPartitions()

    logInfo(s"Found ${partitions.length} clusters.")

    partitions
  }
}
