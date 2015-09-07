
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector

import java.util.HashMap
import java.util.Map
import scala.collection.JavaConversions._
import scala.collection.mutable._
import org.apache.spark._
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.orientechnologies.orient.core.exception.OTransactionException
import com.orientechnologies.orient.core.exception.OConcurrentModificationException
import scala.util.control.Breaks._
import org.apache.spark.SparkContext._

/** Provides OrientDB graph-oriented function on [[org.apache.spark.graphx.Graph]] */
class GraphFunctions[V, E](graph: Graph[V, E]) extends Serializable with Logging {
  /**
   * Converts the instance of [[org.apache.spark.graphx.Graph]] to a [[com.tinkerpop.blueprints.impls.orient.OrientGraph]] instance
   * and saves it into the Orient Database defined in the [[org.apache.spark.SparkContext]].
   */
  def saveGraphToOrient()(implicit connector: OrientDBConnector = OrientDBConnector(graph.vertices.sparkContext.getConf)): Unit = {

    var ograph: OrientGraph = null
    val vertices = graph.vertices.map { vert =>
      //println(vert +  " " + vert._1 + " " + vert._2 )
      ograph = connector.databaseGraphTx()
      val myVClass = getObjClass(vert._2)
      val orientVertex = ograph.addVertex(s"class:$myVClass", toMap(vert._2))
      connector.commit(ograph)
      (vert._1, orientVertex.getId.toString())

    }
      .reduceByKey(_ + _)

    val mappedEdges = graph.edges.map { edge => (edge.srcId, (edge.dstId, edge.attr)) }
      .join { vertices }.map { case (idf, ((idt, attr), vf)) => (idt, ((idf, vf), attr)) }
      .join { vertices }.map { case (idt, (((idf, vf), attr), vt)) => (vf, vt, attr) }

    val edges = mappedEdges.map {
      case (vertexFrom, vertexTo, attr) =>
        ograph = connector.databaseGraphTx()
        val from = ograph.getVertex(vertexFrom)
        val to = ograph.getVertex(vertexTo)
        var retry = 0
        var done = false
        while (retry < 100 && !done) {
          retry = retry + 1
          try {
            val myEClass = getObjClass(attr)
            val e = ograph.addEdge(s"class:$myEClass", from, to, null)
            toMap(attr).foreach {
              case (key, value) =>
                e.setProperty(key, value)
            }

            // ograph.commit()
            connector.commit(ograph)
            done = true

          } catch {
            case e: OConcurrentModificationException =>
              from.reload()
              to.reload()
          }

        }
    }
    println("Saved to OrientDB: " + vertices.count() + " vertices and " + edges.count() + " edges")
  }

  /**
   * Converts an instance of a case class to a Map[String, Object]
   * of its fields and fields values
   * @param myOb an object
   * @return the result Map[String, Object] where keys are
   *             the declared names of class field and values are
   *             field's values of myOb instance
   */
  private def toMap[N](myOb: N): Map[String, Object] = {
    var map: Map[String, Object] = new HashMap()
    if (!myOb.isInstanceOf[java.lang.String]) {
      myOb.getClass().getDeclaredFields.foreach {
        case field =>
          field.setAccessible(true)
          map.+=((field.getName, field.get(myOb)))
      }
    }
    map
  }

  /**
   *  Gets class name of a case class instance.
   *  If the object is a String, returns the value
   *  @param myOb an object
   *  @return myOb belonging class name or string value if myOb is a String
   */

  private def getObjClass(myOb: Any): String = {
    if (myOb.isInstanceOf[java.lang.String]) {
      myOb.asInstanceOf[java.lang.String].toString()
    } else {
      myOb.getClass().getSimpleName
    }
  }

}



