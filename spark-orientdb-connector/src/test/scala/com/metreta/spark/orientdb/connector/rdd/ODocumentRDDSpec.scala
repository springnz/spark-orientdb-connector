
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.rdd

import java.lang.Double
import java.math.BigDecimal
import java.util.{ Calendar, Date }

import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.metreta.spark.orientdb.connector.utils.BaseOrientDbFlatSpec
import com.orientechnologies.orient.core.config.OGlobalConfiguration
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE
import com.orientechnologies.orient.core.metadata.schema.{ OClass, OSchema, OType }
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.commons.codec.binary.Base64

import scala.util.Random

class ODocumentRDDSpec extends BaseOrientDbFlatSpec {

  val dbname = "/tmp/databases/test/ClassRDDSpec"

  var NumInsertLoop = 250
  var NumStringField = 5
  var AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  var rnd = new Random()
  var Counter = 0

  val calendarDateTime = Calendar.getInstance()
  val calendarDate = Calendar.getInstance()
  calendarDate.set(Calendar.HOUR_OF_DAY, 0)
  calendarDate.set(Calendar.MINUTE, 0)
  calendarDate.set(Calendar.SECOND, 0)
  calendarDate.set(Calendar.MILLISECOND, 0)

  override def beforeAll(): Unit = {
    defaultSparkConf.set("spark.orientdb.dbname", dbname)
    initSparkConf(defaultSparkConf)
    buildTestDb
  }

  override def afterAll(): Unit = {
    sparkContext.stop()
  }

  "A ODocumentRDD" should "allow to read an Orient Class as Array of ODocument" in {
    val oDocuments = sparkContext.orientDocumentQuery("class_test").collect()
    oDocuments should have length NumInsertLoop
    val result = OrientDocument.fromODocument(oDocuments.head)
    result.getString("string_field_1") should startWith("text")
    result.getAs[Date]("date_field") should be(calendarDate.getTime)
    result.getAs[Boolean]("boolean_field") should be(true)
    result.getAs[Float]("float_field") should be(3.4.toFloat)
    result.getAs[Int]("int_field") should (be >= 0 and be <= 100)
    result.getAs[Array[Byte]]("binary_field") should be(Array(98, 105, 110, 97, 114, 121, 68, 97, 116, 97))
    result.getAs[Byte]("byte_field") should be(1)
    result.getAs[BigDecimal]("decimal_field") should be(new BigDecimal(1.1))
    result.getAs[Date]("datetime_field") should be(calendarDateTime.getTime)
    result.getAs[Double]("double_field") should be(3.4)
    result.getAs[Long]("long_field") should be(4)
    result.getAs[Short]("short_field") should be(3)
  }

  private def buildTestDb()(implicit connector: OrientDBConnector = OrientDBConnector(defaultSparkConf)) = {

    OGlobalConfiguration.INDEX_AUTO_LAZY_UPDATES.setValue(0) // Turn off cache
    OGlobalConfiguration.INDEX_MANUAL_LAZY_UPDATES.setValue(0)

    val database: ODatabaseDocumentTx = connector.databaseDocumentTxLocal()

    if (database.exists()) {
      if (database.isClosed())
        database.open(connector.user, connector.pass)
      database.drop()
    }

    database.create()

    val schema: OSchema = connector.getSchema(database)

    val klass: OClass = schema.createClass("class_test", database.addCluster("class_test"))

    // create some string fields
    for (i ← 1 to NumStringField) {
      klass.createProperty("string_field_" + i, OType.STRING).setMandatory(true)
    }

    klass.createProperty("date_field", OType.DATE).setMandatory(false)
    klass.createProperty("boolean_field", OType.BOOLEAN).setMandatory(false)
    klass.createProperty("float_field", OType.FLOAT).setMandatory(false)
    klass.createProperty("int_field", OType.INTEGER).setMandatory(false)
    klass.createProperty("binary_field", OType.BINARY).setMandatory(false)
    klass.createProperty("byte_field", OType.BYTE).setMandatory(false)
    klass.createProperty("decimal_field", OType.DECIMAL).setMandatory(false)
    klass.createProperty("datetime_field", OType.DATETIME).setMandatory(false)
    klass.createProperty("double_field", OType.DOUBLE).setMandatory(false)
    klass.createProperty("long_field", OType.LONG).setMandatory(false)
    klass.createProperty("short_field", OType.SHORT).setMandatory(false)

    // create some indexes
    klass.createIndex("index_field_1", INDEX_TYPE.NOTUNIQUE, "string_field_1")
    klass.createIndex("index_field_2", INDEX_TYPE.UNIQUE, "string_field_2")
    klass.createIndex("index_field_3", INDEX_TYPE.DICTIONARY, "string_field_3")

    database.declareIntent(new OIntentMassiveInsert())

    for (i ← 1 to NumInsertLoop) {
      database.save(createRandomDocument())
      println("insert record: #" + i)
    }
  }

  private def createRandomDocument(): ODocument = {
    val document: ODocument = new ODocument("class_test")

    for (i ← 1 to NumStringField) {
      document.field("string_field_" + i, getRandomText(50))
    }
    document.field("binary_field", Base64.encodeBase64String("binaryData".getBytes))
    document.field("date_field", calendarDate.getTime)
    document.field("boolean_field", true)
    document.field("float_field", 3.4)
    document.field("int_field", 24)
    document.field("byte_field", 1.toByte)
    document.field("decimal_field", new BigDecimal(1.1))
    document.field("datetime_field", calendarDateTime.getTime)
    document.field("double_field", 3.4d)
    document.field("long_field", 4.toLong)
    document.field("short_field", 3.toShort)

    document
  }

  private def getRandomText(len: Int): String = {
    var sb = new StringBuilder
    for (i ← 0 to len) {
      sb.append(AB.charAt(rnd.nextInt(AB.length())))
    }
    Counter = Counter + 1
    val ret = "text_" + sb.toString() + " - " + (Counter)
    ret
  }

}
