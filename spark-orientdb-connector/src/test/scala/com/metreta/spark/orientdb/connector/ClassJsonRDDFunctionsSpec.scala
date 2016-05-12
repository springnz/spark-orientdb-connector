package com.metreta.spark.orientdb.connector

import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.metreta.spark.orientdb.connector.utils.BaseOrientDbFlatSpec
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.db.record.{ OTrackedList, OTrackedMap }
import com.orientechnologies.orient.core.metadata.schema.{ OClass, OSchema }
import org.apache.spark.rdd.RDD

import scala.Vector
import scala.collection.JavaConversions.asScalaIterator

class ClassJsonRDDFunctionsSpec extends BaseOrientDbFlatSpec {

  val dbname = "/tmp/databases/test/ClassJsonRDDFunctionsSpec"

  var rddTest: RDD[String] = null
  var database: ODatabaseDocumentTx = null

  override def beforeAll(): Unit = {
    defaultSparkConf.set("spark.orientdb.dbname", dbname)
    initSparkConf(defaultSparkConf)
    rddTest = createRDD()
    database = createDB
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "A spark RDD " should " allow to write a case class RDD to Orient Class on Orient with all fields defined " in {

    rddTest.saveJsonToOrient("class_json_ins_test")

    val result = database.browseClass("class_json_ins_test")
    val vecRes = Vector() ++ result.iterator()

    for (v ← vecRes) {
      v.field("guid").asInstanceOf[String] should
        (be("18026e8b-5382-45b4-bac0-835ddaf4ca12") or be("d458ba2a-f813-42ee-a647-b65fee9af4ee"))
      v.field("intField").asInstanceOf[Int] should (be(28) or be(32))
      val friends = v.field("arrayField").asInstanceOf[OTrackedList[Object]]
      val firstFriend = friends.get(0).asInstanceOf[OTrackedMap[Object]]
      firstFriend.get("id").asInstanceOf[Int] shouldBe 0
      firstFriend.get("name").asInstanceOf[String] should (be("Maggie Ray") or be("Arnold Roman"))
    }
  }

  private def createDB()(implicit connector: OrientDBConnector = OrientDBConnector(sparkContext.getConf)) = {

    val database: ODatabaseDocumentTx = connector.databaseDocumentTxLocal()

    if (database.exists()) {
      if (database.isClosed())
        database.open(connector.user, connector.pass)
      database.drop()
    }

    database.create()

    val schema: OSchema = connector.getSchema(database)
    val klass: OClass = schema.createClass("class_json_ins_test", database.addCluster("class_json_ins_test"))

    database
  }

  private def createRDD() = {
    val json1 = """{
                  |    "_id": "56aad43ffd1e2d6f65341e16",
                  |    "index": 0,
                  |    "guid": "18026e8b-5382-45b4-bac0-835ddaf4ca12",
                  |    "boolField": true,
                  |    "currencyField": "$2,201.55",
                  |    "intField": 28,
                  |    "stringField": "Mcfadden Hawkins",
                  |    "arrayField": [
                  |      {
                  |        "id": 0,
                  |        "name": "Maggie Ray"
                  |      },
                  |      {
                  |        "id": 1,
                  |        "name": "Marsh Roberts"
                  |      },
                  |      {
                  |        "id": 2,
                  |        "name": "Lora Richmond"
                  |      }
                  |    ]
                  |  }
                  |""".stripMargin
    val json2 = """{
                  |    "_id": "56aad43fbd3963a072c4b50d",
                  |    "index": 1,
                  |    "guid": "d458ba2a-f813-42ee-a647-b65fee9af4ee",
                  |    "boolField": false,
                  |    "currencyField": "$3,005.41",
                  |    "intField": 32,
                  |    "stringField": "Clarke Gross",
                  |    "arrayField": [
                  |      {
                  |        "id": 0,
                  |        "name": "Arnold Roman"
                  |      },
                  |      {
                  |        "id": 1,
                  |        "name": "Hudson Goodwin"
                  |      },
                  |      {
                  |        "id": 2,
                  |        "name": "Brady Sharp"
                  |      }
                  |    ]
                  |  }
                  |""".stripMargin

    sparkContext.parallelize(List(json1, json2))

  }

}
