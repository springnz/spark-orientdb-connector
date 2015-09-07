/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.demo

import java.util.Calendar
import java.util.Date
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.Random
import org.apache.spark.SparkConf
import com.metreta.spark.orientdb.connector.demo._
import com.orientechnologies.common.util.OCallable
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraph
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import com.tinkerpop.blueprints.impls.orient.OrientVertex

case class Comment(authorId: String, authorUsername: String, date: Date, rowMessage: String, threadId: String)
case class CommentLike(threadId: String, authorId: String)

object BasicGraphReader extends DemoUtils {

  private def connectionUri() = conf.get(OriendtDBProtocolProperty) + ":/" + conf.get(OriendtDBDBNameProperty)
  private def getText() = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."

  def main(args: Array[String]) {
    init(connectionUri)
    createGraph(connectionUri)
    insertData(connectionUri)

    val graph = sc.orientGraph()
    println("vertices: " + graph.vertices.count())
    println("edges: " + graph.edges.count())

    for (triplet <- graph.triplets.filter(t => t.dstAttr.oClassName.equals("comments") && t.attr.oClassName.equals("likes"))) {
      println(s"triplet.attr: ${triplet.attr}")
      println(s"triplet.dstAttr: ${triplet.dstAttr.getString("authorUsername")}")
    }

    //   graph.triplets.filter(t => t.dstAttr.oClassName.equals("comments") && t.attr.oClassName.equals("likes")).groupBy(_.dstAttr.getAs("authorUsername")) foreach println

  }

  private def insertData(connectionUri: String) {

    val pool: OrientGraphFactory = new OrientGraphFactory(connectionUri, conf.get(OriendtDBUserProperty), conf.get(OriendtDBPasswordProperty)).setupPool(1, 10)
    val session = pool.getTx()

    try {

      val MockThreadId1 = "0"
      val MockThreadId2 = "1"
      val MaxNumComments = 20
      val MaxNumLike = 50

      var commentVertices = new ArrayBuffer[OrientVertex]
      var commentLikeVertices = new ArrayBuffer[OrientVertex]

      session.executeOutsideTx(new OCallable[AnyRef, OrientBaseGraph]() {
        def call(argument: OrientBaseGraph): AnyRef = {
          session.createVertexType("comments").setClusterSelection("default");
          session.createVertexType("commentLikes").setClusterSelection("default");
          session.createEdgeType("replyTo").setClusterSelection("default");
          session.createEdgeType("likes").setClusterSelection("default");
          null;
        }
      })

      //adding some comment vertices
      for (i <- 0 until MaxNumComments) {
        var mockThreadId = MockThreadId1
        if (i % 2 != 0)
          mockThreadId = MockThreadId2
        commentVertices += createComment(session, new Comment(i.toString(), "author(" + i + ")", Calendar.getInstance.getTime, i + " -" + getText, mockThreadId))
      }

      //creating edges - thread 1
      commentVertices(2).addEdge("replyTo", commentVertices(0))
      commentVertices(4).addEdge("replyTo", commentVertices(0))
      commentVertices(6).addEdge("replyTo", commentVertices(0))
      commentVertices(8).addEdge("replyTo", commentVertices(6))
      commentVertices(10).addEdge("replyTo", commentVertices(8))
      commentVertices(12).addEdge("replyTo", commentVertices(10))
      commentVertices(14).addEdge("replyTo", commentVertices(10))
      commentVertices(16).addEdge("replyTo", commentVertices(0))
      commentVertices(18).addEdge("replyTo", commentVertices(16))

      //creating edges - thread 2
      commentVertices(3).addEdge("replyTo", commentVertices(1))
      commentVertices(5).addEdge("replyTo", commentVertices(3))
      commentVertices(7).addEdge("replyTo", commentVertices(5))
      commentVertices(9).addEdge("replyTo", commentVertices(7))
      commentVertices(11).addEdge("replyTo", commentVertices(7))
      commentVertices(13).addEdge("replyTo", commentVertices(11))
      commentVertices(15).addEdge("replyTo", commentVertices(5))
      commentVertices(17).addEdge("replyTo", commentVertices(15))
      commentVertices(19).addEdge("replyTo", commentVertices(15))

      //adding some commentlike vertices
      for (i <- 0 until MaxNumLike) {
        val comment = pickRandComment(commentVertices)
        val commentLike = createCommentLike(session, new CommentLike(comment.getProperty("threadId"), comment.getProperty("authorId")))
        //create edges between commentlike and its comment
        commentLike.addEdge("likes", comment)
      }

    } finally {
      session.shutdown()
    }
  }

  private def init(connectionUri: String) {
    val database: ODatabaseDocumentTx = new ODatabaseDocumentTx(connectionUri)
    if (database.exists()) {
      if (database.isClosed())
        database.open(conf.get(OriendtDBUserProperty), conf.get(OriendtDBPasswordProperty))
      database.drop()
    }
  }

  private def createGraph(connectionUri: String) {
    val graph: OrientGraph = new OrientGraph(connectionUri, conf.get(OriendtDBUserProperty), conf.get(OriendtDBPasswordProperty))
    graph.shutdown()
  }

  private def createComment(session: OrientGraph, comment: Comment): OrientVertex = {
    val vertex: OrientVertex = session.addVertex("comments", "comments")
    vertex.setProperties("authorId", comment.authorId, "authorUsername", comment.authorUsername, "date", comment.date, "rowMessage", comment.rowMessage, "threadId", comment.threadId)
    vertex
  }

  private def createCommentLike(session: OrientGraph, commentLike: CommentLike): OrientVertex = {
    val vertex: OrientVertex = session.addVertex("commentLikes", "commentLikes")
    vertex.setProperties("authorId", commentLike.authorId, "threadId", commentLike.threadId)
    vertex
  }

  private def pickRandComment(comments: ArrayBuffer[OrientVertex]): OrientVertex = comments((new Random).nextInt(comments.length))

}