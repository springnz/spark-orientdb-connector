
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
import com.orientechnologies.orient.core.sql.OCommandSQL
import java.text.SimpleDateFormat
import org.apache.commons.codec.binary.Base64
import com.tinkerpop.blueprints.impls.orient.OrientVertex

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
   * Converts the instance of [[org.apache.spark.graphx.Graph]] to a [[com.tinkerpop.blueprints.impls.orient.OrientGraph]] instance
   * and upserts it into the Orient Database defined in the [[org.apache.spark.SparkContext]].
   */

  def upsertGraphToOrient()(implicit connector: OrientDBConnector = OrientDBConnector(graph.vertices.sparkContext.getConf)): Unit = {

    val vertexPlaceholder = "${vertex}"
    val wherePlaceholder = "${prop}"
    val classPlaceholder = "${class}"
    val fromPlaceholder = "${from}"
    val toPlaceholder = "${to}"
    val selectRid = "SELECT FLATTEN(@rid) FROM " + vertexPlaceholder + " WHERE " + wherePlaceholder + " = "
    val createEdge = "CREATE EDGE " + classPlaceholder + " FROM " + fromPlaceholder + " TO " + toPlaceholder

    graph.vertices
      .foreachPartition(part => {
        part.foreach {
          case v =>
            if (v._2 != null) {

              val fromFirstField = v._2.getClass().getDeclaredFields.apply(0)
              fromFirstField.setAccessible(true)

              val fromQuery = "UPDATE " + getObjClass(v._2) + "  " + getInsertString(v._2) +
                " upsert return after @rid where " + fromFirstField.getName + " = " + fromFirstField.get(v._2)
              val session = connector.databaseDocumentTx()
              try {
                session.command(new OCommandSQL(fromQuery)).execute().asInstanceOf[java.util.ArrayList[Any]]
                session.commit()

              } catch {
                case e: Exception => {
                  println("exception")
                  session.rollback()
                  e.printStackTrace()
                }
              } finally {
                session.release()
                session.close()
              }
            } else {
              println("cannot create vertex")
            }
        }

      })

    graph.edges
      .foreachPartition(part => {
        part.foreach {
          case edge =>
            val session = connector.databaseDocumentTx()
            val attr = toMap(edge.attr)
            val to = edge.dstId.toLong
            val from = edge.srcId.toLong

            val fromQuery = selectRid
              .replace(vertexPlaceholder, attr.get("classFrom").toString)
              .replace(wherePlaceholder, attr.get("propFrom").toString)
            val fromOrid = session.command(new OCommandSQL(fromQuery + from)).execute().asInstanceOf[java.util.ArrayList[Any]]

            val toQuery = selectRid
              .replace(vertexPlaceholder, attr.get("classTo").toString)
              .replace(wherePlaceholder, attr.get("propTo").toString)
            val toOrid = session.command(new OCommandSQL(toQuery + to)).execute().asInstanceOf[java.util.ArrayList[Any]]

            if (fromOrid != null && !fromOrid.isEmpty() && toOrid != null && !toOrid.isEmpty()) {

              val edgeQuery = createEdge
                .replace(classPlaceholder, attr.get("myClass").toString)
                .replace(fromPlaceholder, fromOrid.get(0).toString)
                .replace(toPlaceholder, toOrid.get(0).toString)

              var retry = true
              var n = 100
              while (retry && n > 0) {
                n = n - 1
                try {
                  val edge_orid = session.command(new OCommandSQL(edgeQuery)).execute().asInstanceOf[java.util.ArrayList[Any]]
                  session.commit()
                  retry = false
                  session.release()
                  session.close()
                } catch {
                  case e: Exception => {
                    session.rollback()
                  }
                }
              }
            } else {
              println("cannot create edge from: " + from + " to " + to)
            }
        }
      })
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

  /**
   * Converts an instance of a case class to a string which will
   * be utilized for SQL INSERT command composition.
   *
   * Example:
   * given a case class Person(name: String, surname: String)
   *
   * getInsertString(Person("Larry", "Page")) will return a String: " name = 'Larry', surname = 'Page'"
   *
   * @param orientClass used to obtain the fields types
   * @param obj an object
   * @return a string
   */
  private def getInsertString[T](obj: T): String = {

    var insStr = "SET"

    obj match {
      case o: Int            => insStr = insStr + " value = " + o + ","
      case o: Boolean        => insStr = insStr + " value = " + o + ","
      case o: BigDecimal     => insStr = insStr + " value = " + o + ","
      case o: Float          => insStr = insStr + " value = " + o + ","
      case o: Double         => insStr = insStr + " value = " + o + ","
      case o: java.util.Date => insStr = insStr + " value = date('" + orientDateFormat.format(o) + "'),"
      case o: Short          => insStr = insStr + " value = " + o + ","
      case o: Long           => insStr = insStr + " value = " + o + ","
      case o: String         => insStr = insStr + " value = '" + o + "',"
      case o: Array[Byte]    => insStr = insStr + " value = '" + Base64.encodeBase64String(o.asInstanceOf[Array[Byte]]) + "',"
      case o: Byte           => insStr = insStr + " value = " + o + ","
      case _ => {
        obj.getClass().getDeclaredFields.foreach {
          case field =>
            field.setAccessible(true)
            insStr = insStr + " " + field.getName + " = " + buildValueByType(field.get(obj)) + ","
        }
      }
    }
    insStr.dropRight(1)
  }

  private val orientDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  private def buildValueByType(fieldValue: AnyRef): String = fieldValue match {
    case _: Array[Byte]      => "'" + Base64.encodeBase64String(fieldValue.asInstanceOf[Array[Byte]]) + "'" //"'" + (fieldValue.asInstanceOf[Array[Byte]]) + "'"//
    case _: java.lang.String => "'" + fieldValue + "'"
    case _: java.util.Date   => "date('" + orientDateFormat.format(fieldValue) + "')"
    case _                   => fieldValue.toString()
  }

}



