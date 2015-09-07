
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector

import java.util.Date
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.util.Calendar
import java.util.Date
import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.orientechnologies.orient.core.metadata.schema.OSchema
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.metadata.schema.OProperty
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import scala.Vector
import scala.collection.JavaConversions.asScalaIterator
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.metreta.spark.orientdb.connector.utils.BaseOrientDbFlatSpec
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx

case class vertexProperties(byte_field: Byte,
  binary_field: Array[Byte],
  boolean_field: Boolean,
  //    embedded_field: ORecord,
  //    embeddedlist_field: List[Object],
  //    embeddedmap_field: Map[String, ORecord],
  //    embeddedset_field: Set[Object],    
  decimal_field: BigDecimal,
  float_field: Float,
  date_field: Date,
  datetime_field: Date,
  double_field: Double,
  int_field: Int,
  //    link_field: ORID,
  //    linklist_field: List[Any], //Any extends ORecord
  //    linkmap_field : Map[String, Any], //Any extends ORecord
  //    linkset_field: Set[Any], //Any extends ORecord
  short_field: Short,
  long_field: Long,
  string_field: String)

case class edgeProperties(byte_field: Byte,
  binary_field: Array[Byte],
  boolean_field: Boolean,
  //    embedded_field: ORecord,
  //    embeddedlist_field: List[Object],
  //    embeddedmap_field: Map[String, ORecord],
  //    embeddedset_field: Set[Object],    
  decimal_field: BigDecimal,
  float_field: Float,
  date_field: Date,
  datetime_field: Date,
  double_field: Double,
  int_field: Int,
  //    link_field: ORID,
  //    linklist_field: List[Any], //Any extends ORecord
  //    linkmap_field : Map[String, Any], //Any extends ORecord
  //    linkset_field: Set[Any], //Any extends ORecord
  short_field: Short,
  long_field: Long,
  string_field: String)

class GraphFunctionsSpec extends BaseOrientDbFlatSpec {

  val calendarDateTime = Calendar.getInstance()
  val calendarDate = Calendar.getInstance()
  calendarDate.set(Calendar.HOUR_OF_DAY, 0)
  calendarDate.set(Calendar.MINUTE, 0)
  calendarDate.set(Calendar.SECOND, 0)
  calendarDate.set(Calendar.MILLISECOND, 0)

  var graphDb: OrientGraphNoTx = null

  override def beforeAll(): Unit = {
    initSparkConf(defaultSparkConf)
    graphDb = createDB

    val graphWithProperties = buildGraphWithProperties
    val graphWithoutProperties = buildGraphWithoutProperties

    graphWithProperties.saveGraphToOrient
    graphWithoutProperties.saveGraphToOrient
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "A graphX graph " should " allow to write any graph to OrientDB " in {

    val edges = Vector() ++ graphDb.getEdgesOfClass("edgeProperties").iterator()
    val vertices = Vector() ++ graphDb.getVerticesOfClass("vertexProperties").iterator()

    val edgesNoP = Vector() ++ graphDb.getEdgesOfClass("edgeNoProperties").iterator()
    val verticesNoP = Vector() ++ graphDb.getVerticesOfClass("vertexNoProperties").iterator()

    edges should have length 9
    vertices should have length 10
    edgesNoP should have length 9
    verticesNoP should have length 10
  }

  it should " allow to write on Orient the same data type as the ones in Scala " in {

    val edges = Vector() ++ graphDb.getEdgesOfClass("edgeProperties").iterator()
    val vertices = Vector() ++ graphDb.getVerticesOfClass("vertexProperties").iterator()
    val edgesNoP = Vector() ++ graphDb.getEdgesOfClass("edgeNoProperties").iterator()
    val verticesNoP = Vector() ++ graphDb.getVerticesOfClass("vertexNoProperties").iterator()

    for (v <- vertices) {
      v.getProperty("byte_field").asInstanceOf[Byte] should be(1.toByte)
      v.getProperty("binary_field").asInstanceOf[Array[Byte]] should be("binaryData".getBytes())
      v.getProperty("boolean_field").asInstanceOf[Boolean] should be(true)
      v.getProperty("decimal_field").asInstanceOf[BigDecimal] should (be >= new BigDecimal(1.0) and be < new BigDecimal(2.0))
      v.getProperty("float_field").asInstanceOf[Float] should (be >= 1.0f and be < 2.0f)
      v.getProperty("datetime_field").asInstanceOf[Date] should be(calendarDateTime.getTime)
      v.getProperty("date_field").asInstanceOf[Date] should be(calendarDate.getTime)
      v.getProperty("double_field").asInstanceOf[Double] should (be >= 1.0d and be < 2.0d)
      v.getProperty("int_field").asInstanceOf[Int] should (be >= 0 and be < 10)
      v.getProperty("short_field").asInstanceOf[Short] should (be >= 0.toShort and be < 10.toShort)
      v.getProperty("long_field").asInstanceOf[Long] should (be >= 0.toLong and be < 10.toLong)
      v.getProperty("string_field").asInstanceOf[String] should startWith("string_")
    }

    for (e <- edges) {
      e.getProperty("byte_field").asInstanceOf[Byte] should be(1.toByte)
      e.getProperty("binary_field").asInstanceOf[Array[Byte]] should be("binaryData".getBytes())
      e.getProperty("boolean_field").asInstanceOf[Boolean] should be(true)
      e.getProperty("decimal_field").asInstanceOf[BigDecimal] should (be >= new BigDecimal(1.0) and be < new BigDecimal(2.0))
      e.getProperty("float_field").asInstanceOf[Float] should (be >= 1.0f and be < 2.0f)
      e.getProperty("datetime_field").asInstanceOf[Date] should be(calendarDateTime.getTime)
      e.getProperty("date_field").asInstanceOf[Date] should be(calendarDate.getTime)
      e.getProperty("double_field").asInstanceOf[Double] should (be >= 1.0d and be < 2.0d)
      e.getProperty("int_field").asInstanceOf[Int] should (be >= 0 and be < 10)
      e.getProperty("short_field").asInstanceOf[Short] should (be >= 0.toShort and be < 10.toShort)
      e.getProperty("long_field").asInstanceOf[Long] should (be >= 0.toLong and be < 10.toLong)
      e.getProperty("string_field").asInstanceOf[String] should startWith("string_")
    }

    for (v <- verticesNoP) {
      v.getPropertyKeys.size() should be(0)
    }

    for (e <- edgesNoP) {
      e.getPropertyKeys.size() should be(0)
    }

  }

  private def buildGraphWithProperties()(implicit connector: OrientDBConnector = OrientDBConnector(defaultSparkConf)) = {

    val date = calendarDate.getTime
    val datetime = calendarDateTime.getTime
    val users: RDD[(VertexId, vertexProperties)] =
      sparkContext.parallelize(Array(
        (1L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.0), 1.0f, date, datetime, 1.0d, 0, 0.toShort, 0.toLong, "string_0")),
        (2L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.1), 1.1f, date, datetime, 1.1d, 1, 1.toShort, 1.toLong, "string_1")),
        (3L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.2), 1.2f, date, datetime, 1.2d, 2, 2.toShort, 2.toLong, "string_2")),
        (4L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.3), 1.3f, date, datetime, 1.3d, 3, 3.toShort, 3.toLong, "string_3")),
        (5L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.4), 1.4f, date, datetime, 1.4d, 4, 4.toShort, 4.toLong, "string_4")),
        (6L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.5), 1.5f, date, datetime, 1.5d, 5, 5.toShort, 5.toLong, "string_5")),
        (7L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.6), 1.6f, date, datetime, 1.6d, 6, 6.toShort, 6.toLong, "string_6")),
        (8L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.7), 1.7f, date, datetime, 1.7d, 7, 7.toShort, 7.toLong, "string_7")),
        (9L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.8), 1.8f, date, datetime, 1.8d, 8, 8.toShort, 8.toLong, "string_8")),
        (10L, vertexProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.9), 1.9f, date, datetime, 1.9d, 9, 9.toShort, 9.toLong, "string_9"))))

    val relationships: RDD[Edge[edgeProperties]] =
      sparkContext.parallelize(Seq(
        Edge(1L, 2L, edgeProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.0), 1.0f, date, datetime, 1.0d, 0, 0.toShort, 0.toLong, "string_0")),
        Edge(2L, 3L, edgeProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.1), 1.1f, date, datetime, 1.1d, 1, 1.toShort, 1.toLong, "string_1")),
        Edge(3L, 4L, edgeProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.2), 1.2f, date, datetime, 1.2d, 2, 2.toShort, 2.toLong, "string_2")),
        Edge(4L, 5L, edgeProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.3), 1.3f, date, datetime, 1.3d, 3, 3.toShort, 3.toLong, "string_3")),
        Edge(5L, 6L, edgeProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.4), 1.4f, date, datetime, 1.4d, 4, 4.toShort, 4.toLong, "string_4")),
        Edge(6L, 7L, edgeProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.5), 1.5f, date, datetime, 1.5d, 5, 5.toShort, 5.toLong, "string_5")),
        Edge(7L, 8L, edgeProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.6), 1.6f, date, datetime, 1.6d, 6, 6.toShort, 6.toLong, "string_6")),
        Edge(8L, 9L, edgeProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.7), 1.7f, date, datetime, 1.7d, 7, 7.toShort, 7.toLong, "string_7")),
        Edge(9L, 1L, edgeProperties(1.toByte, "binaryData".getBytes(), true, new BigDecimal(1.8), 1.8f, date, datetime, 1.8d, 8, 8.toShort, 8.toLong, "string_8"))))

    Graph(users, relationships)

  }

  private def buildGraphWithoutProperties()(implicit connector: OrientDBConnector = OrientDBConnector(defaultSparkConf)) = {

    val vertexClass = "vertexNoProperties"
    val edgeClass = "edgeNoProperties"

    val users: RDD[(VertexId, String)] =
      sparkContext.parallelize(Array(
        (1L, vertexClass),
        (2L, vertexClass),
        (3L, vertexClass),
        (4L, vertexClass),
        (5L, vertexClass),
        (6L, vertexClass),
        (7L, vertexClass),
        (8L, vertexClass),
        (9L, vertexClass),
        (10L, vertexClass)))

    val relationships: RDD[Edge[String]] =
      sparkContext.parallelize(Seq(
        Edge(1L, 2L, edgeClass),
        Edge(2L, 3L, edgeClass),
        Edge(3L, 4L, edgeClass),
        Edge(4L, 5L, edgeClass),
        Edge(5L, 6L, edgeClass),
        Edge(6L, 7L, edgeClass),
        Edge(7L, 8L, edgeClass),
        Edge(8L, 9L, edgeClass),
        Edge(9L, 1L, edgeClass)))

    Graph(users, relationships)

  }

  private def createDB()(implicit connector: OrientDBConnector = OrientDBConnector(sparkContext.getConf)) = {

    init(connector)
    createGraph(connector)

    val pool: OrientGraphFactory = new OrientGraphFactory(connector.connStringLocal, connector.user, connector.pass).setupPool(1, 10)
    val graph = pool.getNoTx()

    val vClass = graph.getVertexBaseType
    val vertexNoPClass: OClass = graph.createVertexType("vertexNoProperties", vClass)
    val vertexClass: OClass = graph.createVertexType("vertexProperties", vClass)

    val vertexByteProp: OProperty = vertexClass.createProperty("byte_field", OType.BYTE).setMandatory(false)
    val vertexBinaryProp: OProperty = vertexClass.createProperty("binary_field", OType.BINARY).setMandatory(false)
    val vertexBooleanProp: OProperty = vertexClass.createProperty("boolean_field", OType.BOOLEAN).setMandatory(false)
    //  val vertexEmbeddedProp: OProperty = vertexClass.createProperty("embedded_field", OType.EMBEDDED).setMandatory(false)
    //  val vertexEmbeddedListProp: OProperty = vertexClass.createProperty("embeddedlist_field", OType.EMBEDDEDLIST).setMandatory(false)
    //  val vertexEmbeddedMapProp: OProperty = vertexClass.createProperty("embeddedmap_field", OType.EMBEDDEDMAP).setMandatory(false)
    //  val vertexEmbeddedSetProp: OProperty = vertexClass.createProperty("embeddedset_field", OType.EMBEDDEDSET).setMandatory(false)
    val vertexDecimalProp: OProperty = vertexClass.createProperty("decimal_field", OType.DECIMAL).setMandatory(false)
    val vertexFloatProp: OProperty = vertexClass.createProperty("float_field", OType.FLOAT).setMandatory(false)
    val vertexDateProp: OProperty = vertexClass.createProperty("date_field", OType.DATE).setMandatory(false)
    val vertexDatetimeProp: OProperty = vertexClass.createProperty("datetime_field", OType.DATETIME).setMandatory(false)
    val vertexDoubleProp: OProperty = vertexClass.createProperty("double_field", OType.DOUBLE).setMandatory(false)
    val vertexIntProp: OProperty = vertexClass.createProperty("int_field", OType.INTEGER).setMandatory(false)
    //  val vertexLinkProp: OProperty = vertexClass.createProperty("link_field", OType.LINK).setMandatory(false)
    //  val vertexLinkListProp: OProperty = vertexClass.createProperty("linklist_field", OType.LINKLIST).setMandatory(false)
    //  val vertexLinkMapProp: OProperty = vertexClass.createProperty("linkmap_field", OType.LINKMAP).setMandatory(false)
    //  val vertexLinkSetProp: OProperty = vertexClass.createProperty("linkset_field", OType.LINKSET).setMandatory(false)
    val vertexLongProp: OProperty = vertexClass.createProperty("long_field", OType.LONG).setMandatory(false)
    val vertexShortProp: OProperty = vertexClass.createProperty("short_field", OType.SHORT).setMandatory(false)
    val vertexStringProp: OProperty = vertexClass.createProperty("string_field", OType.STRING).setMandatory(false)

    val eClass = graph.getEdgeBaseType
    val edgeNoPClass: OClass = graph.createEdgeType("edgeNoProperties", eClass)
    val edgeClass: OClass = graph.createEdgeType("edgeProperties", eClass)

    //schema.createClass("edgeProperties", eClass, database.addCluster("edgeProperties"))

    val edgeByteProp: OProperty = edgeClass.createProperty("byte_field", OType.BYTE).setMandatory(false)
    val edgeBinaryProp: OProperty = edgeClass.createProperty("binary_field", OType.BINARY).setMandatory(false)
    val edgeBooleanProp: OProperty = edgeClass.createProperty("boolean_field", OType.BOOLEAN).setMandatory(false)
    //  val edgeEmbeddedProp: OProperty = edgeClass.createProperty("embedded_field", OType.EMBEDDED).setMandatory(false)
    //  val edgeEmbeddedListProp: OProperty = edgeClass.createProperty("embeddedlist_field", OType.EMBEDDEDLIST).setMandatory(false)
    //  val edgeEmbeddedMapProp: OProperty = edgeClass.createProperty("embeddedmap_field", OType.EMBEDDEDMAP).setMandatory(false)
    //  val edgeEmbeddedSetProp: OProperty = edgeClass.createProperty("embeddedset_field", OType.EMBEDDEDSET).setMandatory(false)
    val edgeDecimalProp: OProperty = edgeClass.createProperty("decimal_field", OType.DECIMAL).setMandatory(false)
    val edgeFloatProp: OProperty = edgeClass.createProperty("float_field", OType.FLOAT).setMandatory(false)
    val edgeDateProp: OProperty = edgeClass.createProperty("date_field", OType.DATE).setMandatory(false)
    val edgeDatetimeProp: OProperty = edgeClass.createProperty("datetime_field", OType.DATETIME).setMandatory(false)
    val edgeDoubleProp: OProperty = edgeClass.createProperty("double_field", OType.DOUBLE).setMandatory(false)
    val edgeIntProp: OProperty = edgeClass.createProperty("int_field", OType.INTEGER).setMandatory(false)
    //  val edgeLinkProp: OProperty = edgeClass.createProperty("link_field", OType.LINK).setMandatory(false)
    //  val edgeLinkListProp: OProperty = edgeClass.createProperty("linklist_field", OType.LINKLIST).setMandatory(false)
    //  val edgeLinkMapProp: OProperty = edgeClass.createProperty("linkmap_field", OType.LINKMAP).setMandatory(false)
    //  val edgeLinkSetProp: OProperty = edgeClass.createProperty("linkset_field", OType.LINKSET).setMandatory(false)
    val edgeLongProp: OProperty = edgeClass.createProperty("long_field", OType.LONG).setMandatory(false)
    val edgeShortProp: OProperty = edgeClass.createProperty("short_field", OType.SHORT).setMandatory(false)
    val edgeStringProp: OProperty = edgeClass.createProperty("string_field", OType.STRING).setMandatory(false)

    graph

  }

  def init(connector: OrientDBConnector) {
    val database: ODatabaseDocumentTx = connector.databaseDocumentTxLocal()
    if (database.exists()) {
      if (database.isClosed())
        database.open(connector.user, connector.pass)
      database.drop()
    }
  }

  def createGraph(connector: OrientDBConnector) = {
    val graph: OrientGraph = new OrientGraph(connector.connStringLocal, connector.user, connector.pass)
    graph.shutdown()
  }

}