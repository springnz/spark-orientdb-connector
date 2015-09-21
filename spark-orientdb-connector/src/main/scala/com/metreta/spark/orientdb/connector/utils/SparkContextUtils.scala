
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.utils

import org.apache.spark.{ SparkConf, SparkContext }
import com.metreta.spark.orientdb.connector.SparkContextFunctions

/**
 * Utility to instantiate and manage a [[org.apache.spark.SparkContext]] object.
 *
 */
trait SparkContextUtils {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions = new SparkContextFunctions(sc)

  /**
   * Return the active [[org.apache.spark.SparkContext]].
   */
  def sparkContext: SparkContext = SparkContextUtils.sc

  /**
   * Init a [[org.apache.spark.SparkContext]] with the given configuration.
   */
  def initSparkConf(conf: SparkConf): SparkContext =
    SparkContextUtils.initSparkConf(conf)

  def defaultSparkConf = SparkContextUtils.defaultSparkConf
}

object SparkContextUtils {

  val protocol = "plocal" //"memory"
  val user = "admin"
  val pass = "admin"
  val dbname = "/tmp/databases/test/orientdb"

  /**
   * Default [[org.apache.spark.SparkContext]] configuration.
   */
  val defaultSparkConf = new SparkConf(true)
    .setMaster("local[*]")
    .setAppName("Test")
    .set("spark.orientdb.connection.nodes", "127.0.0.1")
    .set("spark.orientdb.port", "2424")
    .set("spark.orientdb.protocol", protocol)
    .set("spark.orientdb.dbname", dbname)
    .set("spark.orientdb.user", user)
    .set("spark.orientdb.password", pass)
    .set("spark.orientdb.clustermode", "colocated")

  private var _sparkContex: SparkContext = _

  /**
   * Return the active [[org.apache.spark.SparkContext]].
   */
  def sc: SparkContext = _sparkContex

  /**
   * Init a [[org.apache.spark.SparkContext]] with the given configuration.
   */
  def initSparkConf(conf: SparkConf = SparkContextUtils.defaultSparkConf): SparkContext = {
    if (_sparkContex.getConf.getAll.toMap != conf.getAll.toMap)
      resetSparkContext(conf)
    _sparkContex
  }

  private def resetSparkContext(conf: SparkConf) = {
    if (_sparkContex != null)
      _sparkContex.stop()
    _sparkContex = new SparkContext(conf)
    _sparkContex
  }

  resetSparkContext(defaultSparkConf)
}
