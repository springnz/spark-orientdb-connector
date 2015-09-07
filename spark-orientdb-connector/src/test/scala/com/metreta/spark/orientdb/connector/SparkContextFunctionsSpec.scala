
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector

import scala.collection.mutable.ArrayBuffer
import org.scalatest.BeforeAndAfterAll
import com.orientechnologies.orient.core.id.ORID
import com.metreta.spark.orientdb.connector.utils.BaseOrientDbFlatSpec

class SparkContextFunctionsSpec extends BaseOrientDbFlatSpec {

  var oridList: ArrayBuffer[String] = new ArrayBuffer
  var MaxCluster = 1000
  var MaxRecord = 1000

  override def beforeAll(): Unit = {
    initSparkConf(defaultSparkConf)
    createOridList()
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "A VertexId created from RID" should "be unique" in {
    val vertexIdList = oridList map { rid => sparkContext.getVertexIdFromString(rid) }
    val duplicatedValues = vertexIdList.groupBy(identity).collect { case (x, ys) if ys.lengthCompare(1) > 0 => x }
    duplicatedValues shouldBe empty
  }

  it should "be a positive number" in {
    val negativeValues = oridList filter { rid => sparkContext.getVertexIdFromString(rid) < 0 }
    negativeValues shouldBe empty
  }

  def createOridList() {
    for (clusterId <- 0 to MaxCluster) {
      for (recordId <- 0 to MaxRecord) {
        val rid = new StringBuilder
        rid.append(ORID.PREFIX);
        rid.append(clusterId);
        rid.append(ORID.SEPARATOR);
        rid.append(recordId);
        oridList += rid.toString
      }
    }
  }

}