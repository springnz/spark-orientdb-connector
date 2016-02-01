
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector

import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.util.Calendar
import java.util.Date
import scala.Vector
import scala.collection.JavaConversions.asScalaIterator
import org.apache.spark.rdd.RDD
import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OProperty
import com.orientechnologies.orient.core.metadata.schema.OSchema
import com.orientechnologies.orient.core.metadata.schema.OType
import com.metreta.spark.orientdb.connector.utils.BaseOrientDbFlatSpec

case class BinaryClassInsTest(binary_field: Array[Byte])

case class SmallClassInsTest(int_field: Int,
  float_field: Float,
  boolean_field: Boolean,
  string_field: String)

case class ClassInsTest(
  byte_field: Byte,
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

class ClassRDDFunctionsSpec extends BaseOrientDbFlatSpec {
  
  val dbname = "/tmp/databases/test/ClassRDDFunctionsSpec"

  val calendarDateTime = Calendar.getInstance()
  val calendarDate = calendarDateTime
  calendarDate.set(Calendar.HOUR_OF_DAY, 0)
  calendarDate.set(Calendar.MINUTE, 0)
  calendarDate.set(Calendar.SECOND, 0)
  calendarDate.set(Calendar.MILLISECOND, 0)

  var rddTest: RDD[ClassInsTest] = null
  var database: ODatabaseDocumentTx = null

  //sparkContext.orientQuery("class_ins_test").foreach(println)
  
  override def beforeAll(): Unit = {
    defaultSparkConf.set("spark.orientdb.dbname", dbname)
    initSparkConf(defaultSparkConf)

    rddTest = createRDD

    database = createDB

    rddTest.saveToOrient("class_ins_test")
    rddTest.saveToOrient("class_ins_test_no_def")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "A spark RDD " should " allow to write a case class RDD to Orient Class on Orient with all fields defined " in {

    val result = database.browseClass("class_ins_test")
    val vecRes = Vector() ++ result.iterator()
    vecRes should have length 3
  }

  it should " allow to write on an Orient Class with all fields defined the same data type as the ones in Scala " in {

    val result = database.browseClass("class_ins_test")
    val vecRes = Vector() ++ result.iterator()

    for (v <- vecRes) {

      v.field("byte_field").asInstanceOf[Byte] should be(1.toByte)
      v.field("binary_field").asInstanceOf[Array[Byte]] should be("binaryData".getBytes())
      v.field("boolean_field").asInstanceOf[Boolean] should be(true)
      v.field("decimal_field").asInstanceOf[BigDecimal] should (be > new BigDecimal(1.0) and be < new BigDecimal(3.4))
      v.field("float_field").asInstanceOf[Float] should (be > 1.0f and be < 3.4f)
      v.field("datetime_field").asInstanceOf[Date] should be(calendarDateTime.getTime)
      v.field("date_field").asInstanceOf[Date] should be(calendarDate.getTime)
      v.field("double_field").asInstanceOf[Double] should (be > 1.0d and be < 3.4d)
      v.field("int_field").asInstanceOf[Int] should (be > 0 and be < 4)
      v.field("short_field").asInstanceOf[Short] should (be > 0.toShort and be < 4.toShort)
      v.field("long_field").asInstanceOf[Long] should (be > 0.toLong and be < 4.toLong)
      v.field("string_field").asInstanceOf[String] should startWith("string_")

    }

  }

  it should " allow to write a case class RDD to Orient Class on Orient with all fields not defined " in {

    val result = database.browseClass("class_ins_test_no_def")
    val vecRes = Vector() ++ result.iterator()
    vecRes should have length 3
  }

  it should " allow to write on an Orient Class with all fields not defined the same data type as the ones in Scala " in {

    val result = database.browseClass("class_ins_test_no_def")
    val vecRes = Vector() ++ result.iterator()

    for (v <- vecRes) {

      v.field("byte_field").asInstanceOf[Byte] should be(1.toByte)
      v.field("binary_field").asInstanceOf[Array[Byte]] should be("binaryData".getBytes())
      v.field("boolean_field").asInstanceOf[Boolean] should be(true)
      v.field("decimal_field").asInstanceOf[BigDecimal] should (be > new BigDecimal(1.0) and be < new BigDecimal(3.4))
      v.field("float_field").asInstanceOf[Float] should (be > 1.0f and be < 3.4f)
      v.field("datetime_field").asInstanceOf[Date] should be(calendarDateTime.getTime)
      v.field("date_field").asInstanceOf[Date] should be(calendarDate.getTime)
      v.field("double_field").asInstanceOf[Double] should (be > 1.0d and be < 3.4d)
      v.field("int_field").asInstanceOf[Int] should (be > 0 and be < 4)
      v.field("short_field").asInstanceOf[Short] should (be > 0.toShort and be < 4.toShort)
      v.field("long_field").asInstanceOf[Long] should (be > 0.toLong and be < 4.toLong)
      v.field("string_field").asInstanceOf[String] should startWith("string_")

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
    val klass: OClass = schema.createClass("class_ins_test", database.addCluster("class_ins_test"))
    val klassnd: OClass = schema.createClass("class_ins_test_no_def", database.addCluster("class_ins_test_no_def"))

    val byteProp: OProperty = klass.createProperty("byte_field", OType.BYTE).setMandatory(false)
    val binaryProp: OProperty = klass.createProperty("binary_field", OType.BINARY).setMandatory(false)
    val booleanProp: OProperty = klass.createProperty("boolean_field", OType.BOOLEAN).setMandatory(false)
    //  val embeddedProp: OProperty = klass.createProperty("embedded_field", OType.EMBEDDED).setMandatory(false)
    //  val embeddedListProp: OProperty = klass.createProperty("embeddedlist_field", OType.EMBEDDEDLIST).setMandatory(false)
    //  val embeddedMapProp: OProperty = klass.createProperty("embeddedmap_field", OType.EMBEDDEDMAP).setMandatory(false)
    //  val embeddedSetProp: OProperty = klass.createProperty("embeddedset_field", OType.EMBEDDEDSET).setMandatory(false)
    val decimalProp: OProperty = klass.createProperty("decimal_field", OType.DECIMAL).setMandatory(false)
    val floatProp: OProperty = klass.createProperty("float_field", OType.FLOAT).setMandatory(false)
    val dateProp: OProperty = klass.createProperty("date_field", OType.DATE).setMandatory(false)
    val datetimeProp: OProperty = klass.createProperty("datetime_field", OType.DATETIME).setMandatory(false)
    val doubleProp: OProperty = klass.createProperty("double_field", OType.DOUBLE).setMandatory(false)
    val intProp: OProperty = klass.createProperty("int_field", OType.INTEGER).setMandatory(false)
    //  val linkProp: OProperty = klass.createProperty("link_field", OType.LINK).setMandatory(false)
    //  val linkListProp: OProperty = klass.createProperty("linklist_field", OType.LINKLIST).setMandatory(false)
    //  val linkMapProp: OProperty = klass.createProperty("linkmap_field", OType.LINKMAP).setMandatory(false)
    //  val linkSetProp: OProperty = klass.createProperty("linkset_field", OType.LINKSET).setMandatory(false)
    val longProp: OProperty = klass.createProperty("long_field", OType.LONG).setMandatory(false)
    val shortProp: OProperty = klass.createProperty("short_field", OType.SHORT).setMandatory(false)
    val stringProp: OProperty = klass.createProperty("string_field", OType.STRING).setMandatory(false)
    //Transient, Custom, LinkBag, Any => in doc but not available on orient console 

    database
  }

  private def createRDD() = {

    val date = calendarDateTime.getTime

    val cit0 = ClassInsTest(1.toByte,
      "binaryData".getBytes(),
      true, new BigDecimal(1.1), 1.1f, date, date, 1.1d, 1, 1.toShort, 1.toLong, "string_1")
    val cit1 = ClassInsTest(1.toByte,
      "binaryData".getBytes(),
      true, new BigDecimal(2.2), 2.2f, date, date, 2.2d, 2, 2.toShort, 2.toLong, "string_2")
    val cit2 = ClassInsTest(1.toByte,
      "binaryData".getBytes(),
      true, new BigDecimal(3.3), 3.3f, date, date, 3.3d, 3, 3.toShort, 3.toLong, "string_3")

    sparkContext.parallelize(List(cit0, cit1, cit2))

  }

}


class ClassJsonRDDFunctionsSpec extends BaseOrientDbFlatSpec {

  val dbname = "/tmp/databases/test/ClassJsonRDDFunctionsSpec"

  val calendarDateTime = Calendar.getInstance()
  val calendarDate = calendarDateTime
  calendarDate.set(Calendar.HOUR_OF_DAY, 0)
  calendarDate.set(Calendar.MINUTE, 0)
  calendarDate.set(Calendar.SECOND, 0)
  calendarDate.set(Calendar.MILLISECOND, 0)

  var rddTest: RDD[String] = null
  var database: ODatabaseDocumentTx = null

  //sparkContext.orientQuery("class_ins_test").foreach(println)

  override def beforeAll(): Unit = {
    defaultSparkConf.set("spark.orientdb.dbname", dbname)
    initSparkConf(defaultSparkConf)

    rddTest = createRDD

    database = createDB

    rddTest.saveJsonToOrient("class_json_ins_test")
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "A spark RDD " should " allow to write a case class RDD to Orient Class on Orient with all fields defined " in {

    val result = database.browseClass("class_json_ins_test")
    val vecRes = Vector() ++ result.iterator()

    for (v <- vecRes) {
      v.field("guid").asInstanceOf[String] should
        (be("18026e8b-5382-45b4-bac0-835ddaf4ca12") or be("d458ba2a-f813-42ee-a647-b65fee9af4ee"))
      v.field("infField").asInstanceOf[Int] should (be(28) or be(32))
      val arrayField = v.field("arrayField")
      val a = arrayField

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
    //
    sparkContext.parallelize(List(json1, json2))

  }

}