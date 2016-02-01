package com.metreta.spark.orientdb.connector

import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

class ClassJsonRDDFunctions(rdd: RDD[String]) extends Serializable with Logging {
  /**
    * Saves an instance of [[org.apache.spark.rdd.RDD RDD]] into an OrientDB class.
    * @param myClass the OrientDB class to save in
    */
  /*
	 * Prerequisites:
	 *  -> Input class must have been created on OrientDB
	 *  -> rdd must be composed of strings in Json format
	 */ def saveJsonToOrient(myClass: String)(implicit connector: OrientDBConnector = OrientDBConnector(rdd.sparkContext.getConf)): Unit = {
    rdd.foreachPartition { partition ⇒
      val db = connector.databaseDocumentTx()

      while (partition.hasNext) {
        val obj = partition.next()
        val doc = new ODocument(myClass);
        doc.fromJSON(obj)
        db.save(doc)

      }
      db.commit()
      db.close()
    }
  }
}
