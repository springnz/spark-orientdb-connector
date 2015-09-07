
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.rdd

import scala.collection.JavaConversions._
import java.util.Date

abstract class OrientEntry(val className: String,
  val rid: String,
  val columnNames: IndexedSeq[String],
  val columnValues: IndexedSeq[Any]) extends Serializable {

  def getString(index: Int): String = {
    try {
      return columnValues.get(index).asInstanceOf[String]
    } catch {
      case e1: IndexOutOfBoundsException =>
        throw OrientDocumentException("Index not found, please use indices between 0 and " + (columnNames.size - 1))
    }
  }

  def getInt(index: Int): Int = {
    return getString(index).toInt
  }

  def getDouble(index: Int): Double = {
    return getString(index).toDouble
  }

  def getFloat(index: Int): Float = {
    return getString(index).toFloat
  }

  def getBoolean(index: Int): Boolean = {
    return getString(index).toBoolean
  }

  def getShort(index: Int): Short = {
    return getString(index).toShort
  }

  def getLong(index: Int): Long = {
    return getString(index).toLong
  }

  def getByte(index: Int): Byte = {
    return getString(index).toByte
  }

  def getDate(index: Int): Date = {
    return new Date(getString(index))
  }

  def getAs[T](index: Int): T = {
    try {
      return columnValues.get(index).asInstanceOf[T]
    } catch {
      case e1: IndexOutOfBoundsException =>
        throw OrientDocumentException("Index not found, please use indices between 0 and " + (columnNames.size - 1))
    }
  }

  def getString(name: String): String = {
    try {
      return columnValues.get((columnNames.zipWithIndex filter {
        case (columnName, i) => columnName.toLowerCase() == name.toLowerCase()
      }).get(0)._2).asInstanceOf[String]

    } catch {
      case e1: IndexOutOfBoundsException =>
        throw OrientDocumentException(s"Column name not found, please use one of the following names: [${columnNames.mkString("|")}]")
    }

  }

  def getDouble(name: String): Double = {
    return getString(name).toDouble
  }

  def getInt(name: String): Int = {
    return getString(name).toInt
  }

  def getFloat(name: String): Float = {
    return getString(name).toFloat
  }

  def getBoolean(name: String): Boolean = {
    return getString(name).toBoolean
  }

  def getShort(name: String): Short = {
    return getString(name).toShort
  }

  def getLong(name: String): Long = {
    return getString(name).toLong
  }

  def getByte(name: String): Byte = {
    return getString(name).toByte
  }

  def getDate(name: String): Date = {
    return new Date(getString(name))
  }

  def getAs[T](name: String): T = {
    try {
      return columnValues.get((columnNames.zipWithIndex filter {
        case (columnName, i) => columnName.toLowerCase() == name.toLowerCase()
      }).get(0)._2).asInstanceOf[T]

    } catch {
      case e1: IndexOutOfBoundsException =>
        throw OrientDocumentException(s"Column name not found, please use one of the following names: [${columnNames.mkString("|")}]")
    }
  }
}