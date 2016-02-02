
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.rdd.partitioner

import scala.collection.JavaConversions.iterableAsScalaIterable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.Partition

import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OSchema
import com.orientechnologies.orient.core.storage.OStorage
import com.metreta.spark.orientdb.connector.SystemTables
import scala.collection.JavaConversions.iterableAsScalaIterable

/**
  * @author Simone Bronzin
  */
class ClassRDDPartitioner(
    connector: OrientDBConnector,
    mClass: String) extends Logging {

  /**
    * @return Spark partitions from a given OrientdDB class.
    */
  def getPartitions(): Array[Partition] = {

    val db = connector.databaseDocumentTx()

    var partitions = new ArrayBuffer[OrientPartition]
    val schema: OSchema = connector.getSchema(db)
    var klass: OClass = schema.getClass(mClass)
    val storage: OStorage = connector.getStorage(db)
    //TODO: trovare matching corretto tra nodo/host e cluster
    // val delegateFieldValues = storage.asInstanceOf[OStorageRemoteThread].getClusterConfiguration.fieldValues()(0)
    // val nodesHost = delegateFieldValues.asInstanceOf[OTrackedList[ODocument]].map(z => (z.rawField("name"), z.rawField("listeners").asInstanceOf[OTrackedList[OTrackedMap[ODocument]]].get(0).get("listen")))
    //println("cluster selection: " + klass.getClusterSelection.getName)
    klass.getClusterIds.zipWithIndex foreach {
      case (clusterId, index) ⇒ partitions = partitions.+=(OrientPartition(
        index,
        null, // <- Host Address ?????
        PartitionName(klass.getName, storage.getClusterById(clusterId).getName)))
    }
    partitions.toArray
  }

}
