
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.rdd

import java.util.Calendar
import scala.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import com.metreta.spark.orientdb.connector.SparkContextFunctions
import com.orientechnologies.orient.core.config.OGlobalConfiguration
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.OProperty
import com.orientechnologies.orient.core.metadata.schema.OSchema
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import java.util.Date
import java.text.SimpleDateFormat
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import com.tinkerpop.blueprints.impls.orient.OrientVertex
import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import org.apache.spark.graphx.Edge
import com.metreta.spark.orientdb.connector.utils.BaseOrientDbFlatSpec

class GraphRDDSpec extends BaseOrientDbFlatSpec {

  override def beforeAll(): Unit = {
    initSparkConf(defaultSparkConf)
    buildTestDb
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "A GraphRDD" should "allow to build a graphRDD from vertexRDD and edgeRDD" in {
    val result = sparkContext.orientGraph()
    println("vertici: " + result.vertices.count())
    println("archi: " + result.edges.count())
    result.vertices.count() shouldEqual 5
    result.edges.count() shouldEqual 5
  }

  it should "allow to read Orient Vertex Array as vertexRDD[Long, OrientDocument]" in {
    val result = sparkContext.orientGraph()
    val vertexList = result.vertices.collect()
    vertexList(0)._1 shouldBe a[java.lang.Long]
    vertexList(0)._2 shouldBe a[OrientDocument]
    vertexList(0)._2.getString("attr1") should startWith("row")
    vertexList(0)._2.getInt("attr2") should (be > 0 and be < 200)
  }

  it should "allow to read Orient Edge Array as edgeRDD[Edge[OrientDocument]]" in {
    val result = sparkContext.orientGraph()
    val edgeList = result.edges.collect()
    edgeList(0) shouldBe a[Edge[_]]
    edgeList(0).attr shouldBe a[OrientDocument]
    edgeList(0).dstId shouldBe a[java.lang.Long]
    edgeList(0).srcId shouldBe a[java.lang.Long]
  }

  "A GraphRDD obtained by traversing a graph" should "be smaller or equal to the original graph" in {
    val subGraph = sparkContext.traverseGraph("node1", 2)
    val graph = sparkContext.orientGraph()
    println("sub vertici: " + subGraph.vertices.count())
    println("sub archi: " + subGraph.edges.count())
    subGraph.vertices.count() should be <= graph.vertices.count()
    subGraph.edges.count() should be <= graph.edges.count()
  }

  private def buildTestDb()(implicit connector: OrientDBConnector = OrientDBConnector(defaultSparkConf)) = {

    init(connector)
    createGraph(connector)

    val pool: OrientGraphFactory = new OrientGraphFactory(connector.connStringLocal, connector.user, connector.pass).setupPool(1, 10)
    val graph = pool.getTx()

    try {

      val row1Node1: OrientVertex = createNode1(graph, "row1_node1", 32)
      val row2Node1: OrientVertex = createNode1(graph, "row2_node1", 40)
      val row3Node1: OrientVertex = createNode1(graph, "row3_node1", 30)

      row1Node1.addEdge("knows", row2Node1)
      row2Node1.addEdge("knows", row3Node1)
      row3Node1.addEdge("knows", row2Node1)

      val row1Node2: OrientVertex = createNode2(graph, "row1_node2", 55)
      val row1Node3: OrientVertex = createNode3(graph, "row1_node3", 60)

      row1Node1.addEdge("knows", row1Node2)
      row1Node2.addEdge("knows", row1Node3)

    } finally {
      graph.shutdown()
    }

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

  def createNode1(graph: OrientGraph, attr1: String, attr2: Int): OrientVertex = {
    val vertex: OrientVertex = graph.addVertex("node1", "node1")
    vertex.setProperties("attr1", attr1, "attr2", attr2.toString())
    vertex
  }

  def createNode2(graph: OrientGraph, attr1: String, attr2: Int): OrientVertex = {
    val vertex: OrientVertex = graph.addVertex("node2", "node2")
    vertex.setProperties("attr1", attr1, "attr2", attr2.toString())
    vertex
  }

  def createNode3(graph: OrientGraph, attr1: String, attr2: Int): OrientVertex = {
    val vertex: OrientVertex = graph.addVertex("node3", "node3")
    vertex.setProperties("attr1", attr1, "attr2", attr2.toString())
    vertex
  }
}