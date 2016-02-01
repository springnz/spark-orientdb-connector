
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector

import java.math.BigDecimal
import java.text.SimpleDateFormat

import com.metreta.spark.orientdb.connector.api.OrientDBConnector
import com.orientechnologies.orient.core.metadata.schema.OType
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.OCommandSQL
import org.apache.commons.codec.binary.Base64
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

class ClassRDDFunctions[T](rdd: RDD[T]) extends Serializable with Logging {
  /**
   * Saves an instance of [[org.apache.spark.rdd.RDD RDD]] into an OrientDB class.
   * @param myClass the OrientDB class to save in
   */
  /*
	 * Prerequisites:
	 *  -> Input class must have been created on OrientDB
	 *  -> rdd must be composed by instances of a case class or of primitive objects
	 *  -> case class can have a number of attribute >= 0
	 */

  def saveToOrient(myClass: String)(implicit connector: OrientDBConnector = OrientDBConnector(rdd.sparkContext.getConf)): Unit = {

    rdd.foreachPartition { partition =>
      val db = connector.databaseDocumentTx()

      while (partition.hasNext) {
        val obj = partition.next()
        val doc = new ODocument(myClass);
        setProperties("value", doc, obj)
        db.save(doc)

      }
      db.commit()
      db.close()
    }

  }

  /**
   * Upserts an instance of [[org.apache.spark.rdd.RDD RDD]] into an OrientDB class.
   * @param myClass the OrientDB class to save in
   */

  def upsertToOrient(myClass: String)(implicit connector: OrientDBConnector = OrientDBConnector(rdd.sparkContext.getConf)): Unit = {

    rdd.foreachPartition { partition =>
      val db = connector.databaseDocumentTx()

      while (partition.hasNext) {
        val obj = partition.next()
        val fromFirstField = obj.getClass().getDeclaredFields.apply(0)
        fromFirstField.setAccessible(true)

        val fromQuery = "UPDATE " + myClass + "  " + getInsertString(obj) +
          " upsert return after @rid where " + fromFirstField.getName + " = " + fromFirstField.get(obj)
        try {
          db.command(new OCommandSQL(fromQuery)).execute().asInstanceOf[java.util.ArrayList[Any]]
        } catch {
          case e: Exception => {
            db.rollback()
            e.printStackTrace()
          }
        }
      }
      db.commit()
      db.close()
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

  private def setProperties[T](fieldName: String, doc: ODocument, obj: T): Unit = {

    obj match {
      case o: Int            => doc.field(fieldName, o, OType.INTEGER)
      case o: Boolean        => doc.field(fieldName, o, OType.BOOLEAN)
      case o: BigDecimal     => doc.field(fieldName, o, OType.DECIMAL)
      case o: Float          => doc.field(fieldName, o, OType.FLOAT)
      case o: Double         => doc.field(fieldName, o, OType.DOUBLE)
      case o: java.util.Date => doc.field(fieldName, orientDateFormat.format(o), OType.DATE)
      case o: Short          => doc.field(fieldName, o, OType.SHORT)
      case o: Long           => doc.field(fieldName, o, OType.LONG)
      case o: String         => doc.field(fieldName, o, OType.STRING)
      case o: Array[Byte]    => doc.field(fieldName, o, OType.BINARY) //insStr = insStr + " value = '" + Base64.encodeBase64String(o.asInstanceOf[Array[Byte]]) + "',"  
      case o: Byte           => doc.field(fieldName, o, OType.BYTE)
      case _ => {
        obj.getClass().getDeclaredFields.foreach {
          case field =>
            field.setAccessible(true)
            setProperties(field.getName, doc, field.get(obj))
        }
      }
    }

  }

}