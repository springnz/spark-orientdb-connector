
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector

import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.metreta.spark.orientdb.connector.rdd._
import com.orientechnologies.orient.core.id.{ ORID, ORecordId }
import com.orientechnologies.orient.core.metadata.schema.{ OClass, OSchema }
import com.orientechnologies.orient.core.sql.OCommandSQL
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import org.apache.spark.graphx.{ Edge, Graph }
import org.apache.spark.{ Logging, SparkContext }

import scala.collection.JavaConversions.{ collectionAsScalaIterable, _ }

class SparkContextFunctions(@transient val sc: SparkContext) extends Serializable with Logging {

  val out: Int = 0
  val in: Int = 1

  val errRid: Int = -1

  /**
    * @param from: orientDB class name
    * @param where: filter
    * @param connector
    * @return a classRDD
    */
  def orientQuery(from: String, where: String = "")(implicit connector: OrientDBConnector = OrientDBConnector(sc.getConf)) =
    new OrientClassRDD[OrientDocument](sc, connector, from, where)

  def oDocumentQuery(from: String, where: String = "")(implicit connector: OrientDBConnector = OrientDBConnector(sc.getConf)) =
    new ODocumentRDD(sc, connector, from, where)

  /**
    * Creates a [[org.apache.spark.graphx.Graph]] starting by traversing an orientDB class.
    * @param from: orientDB class name
    * @param depth: traverse depth
    * @param connector
    * @return a graphRDD
    */
  def traverseGraph(from: String, depth: Int)(implicit connector: OrientDBConnector = OrientDBConnector(sc.getConf)) = {

    val classRDD = new OrientClassRDD[OrientDocument](sc, connector, from, "", Some(depth))

    val edgeRDD = classRDD.filter {
      case e ⇒
        val dbGraph = connector.databaseGraphTx()
        val schema = connector.getSchema(dbGraph.getRawGraph)
        isEdgeClass(schema.getClass(e.oClassName), dbGraph)
    }.map(orientDocument ⇒ Edge(getVertexIdFromString(orientDocument.columnValues.get(out).toString()),
      getVertexIdFromString(orientDocument.columnValues.get(in).toString()), orientDocument))

    val vertexRDD = classRDD.filter {
      case v ⇒
        val dbGraph = connector.databaseGraphTx()
        val schema = connector.getSchema(dbGraph.getRawGraph)
        isVertexClass(schema.getClass(v.oClassName), dbGraph)
    }.map(orientDocument ⇒ (getVertexIdFromString(orientDocument.rid), orientDocument))

    Graph(vertexRDD, edgeRDD)

  }

  /**
    * Creates a [[org.apache.spark.graphx.Graph]] from a [[com.tinkerpop.blueprints.impls.orient.OrientGraph]]
    * @param connector
    * @return a graphRDD
    */
  def orientGraph()(implicit connector: OrientDBConnector = OrientDBConnector(sc.getConf)) = {
    //new GraphRDD[OrientGraph](sc, connector, query)

    val dbGraph = connector.databaseGraphTx()
    val dbDocument = dbGraph.getRawGraph
    val schema: OSchema = connector.getSchema(dbDocument)

    //    val classes = schema.getClasses.filterNot(x => SystemTable.contains(x.getName))

    var verticesClass: Iterable[OClass] = Nil
    var edgesClass: Iterable[OClass] = Nil

    //orientdb classes list
    val classes = schema.getClasses

    verticesClass = classes filter { isVertexClass(_, dbGraph) }

    edgesClass = classes filter { isEdgeClass(_, dbGraph) }

    // Vertices OrientDocument ClassRDD list
    val vertexClassRDDList = verticesClass map { klass ⇒ new OrientClassRDD[OrientDocument](sc, connector, klass.getName) }

    // Edges OrientDocument ClassRDD list
    val edgeClassRDDList = edgesClass map { klass ⇒ new OrientClassRDD[OrientDocument](sc, connector, klass.getName) }

    //vertex RDD list
    val vertexRDDList = vertexClassRDDList.map(classRDD ⇒ classRDD.map(orientDocument ⇒ (getVertexIdFromString(orientDocument.rid), orientDocument)))

    //edge RDD list
    val edgeRDDList = edgeClassRDDList.map(classRDD ⇒ classRDD.map(orientDocument ⇒ Edge(getVertexIdFromString(orientDocument.columnValues.get(out).toString()),
      getVertexIdFromString(orientDocument.columnValues.get(in).toString()), orientDocument)))

    //union
    //VertexRDD merge
    val vertexRDD = vertexRDDList reduceLeft { _ ++ _ }
    //EdgeRDD merge
    val edgeRDD = edgeRDDList reduceLeft { _ ++ _ }

    val index = edgeRDDList.map(_.partitions.length).sum

    val graph: Graph[OrientDocument, OrientDocument] = Graph(vertexRDD, edgeRDD)

    graph
  }

  /**
    * Executes a single SQL statement on OrientDB
    * @param sqlStatement
    *
    */
  def orientSQLStatement(sqlStatement: String)(implicit connector: OrientDBConnector = OrientDBConnector(sc.getConf)): Unit = {
    val session = connector.databaseDocumentTx()
    try {
      connector.executeCommand(session, new OCommandSQL(sqlStatement))
      session.commit()
    } catch {
      case e: Exception ⇒ session.rollback()
    } finally {
      session.close()
    }
  }

  /**
    * Executes a single SQL statement on OrientDB
    * @param sqlStatement
    * @return the result of the query as list
    */

  def list(sqlStatement: String)(implicit connector: OrientDBConnector = OrientDBConnector(sc.getConf)): java.util.ArrayList[Any] = {

    val session = connector.databaseDocumentTx()
    var res: java.util.ArrayList[Any] = null
    try {
      res = session.command(new OCommandSQL(sqlStatement)).execute().asInstanceOf[java.util.ArrayList[Any]]
      session.commit()
    } catch {
      case e: Exception ⇒ session.rollback()
    } finally {
      session.close()
    }
    res
  }

  /**
    * Defines the [[com.orientechnologies.orient.core.metadata.schema.OClass]] parameter nature
    * @param klass
    * @param dbGraph
    * @return true if the klass param is vertex
    */
  private def isVertexClass(klass: OClass, dbGraph: OrientGraph) = klass match {
    case x if x.isSubClassOf(dbGraph.getVertexBaseType.getName) ⇒ true
    case _ ⇒ false
  }
  /**
    * Define the [[com.orientechnologies.orient.core.metadata.schema.OClass]] parameter nature
    * @param klass
    * @param dbGraph
    * @return true if the klass param is edge
    */
  private def isEdgeClass(klass: OClass, dbGraph: OrientGraph) = klass match {
    case x if x.isSubClassOf(dbGraph.getEdgeBaseType.getName) ⇒ true
    case _ ⇒ false
  }

  /**
    * Transform a [[com.orientechnologies.orient.core.id.ORID]] in a Long
    * @param ridStr
    * @return the unique Long value linked to the the given param
    */
  def getVertexIdFromString(ridStr: String): Long = convertRidToLong(new ORecordId(ridStr))

  private def convertRidToLong(rid: ORID): Long = rid match {
    case x if isValidRid(x) ⇒ hashConcat(rid)
    case _                  ⇒ errRid
  }

  private def hashConcat(rid: ORID): Long = (rid.getClusterId << 16) + rid.getClusterPosition

  private def isValidRid(rid: ORID) = rid match {
    case x if x.isValid() ⇒ true
    case _                ⇒ false
  }

}
