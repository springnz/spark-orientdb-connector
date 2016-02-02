
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb

import scala.language.implicitConversions

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import com.orientechnologies.orient.core.metadata.schema.OClass
import com.orientechnologies.orient.core.metadata.schema.OClassImpl


package object connector {

  val SystemTables = Array[String]("OUser", "ORole", "OIdentity", "ORIDs", "ORestricted", "OFunction", "OTriggered", "OSchedule", "V", "E")

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)
  
  implicit def toOClassImpl(klass: OClass): OClassImpl = klass.asInstanceOf[OClassImpl]
  
  implicit def toRDDFunctions[T](rdd: RDD[T]): ClassRDDFunctions[T] =
    new ClassRDDFunctions(rdd)

  implicit def toJsonRDDFunctions(rdd: RDD[String]): ClassJsonRDDFunctions =
    new ClassJsonRDDFunctions(rdd)

  implicit def toGraphxFunctions[V,E](graph: Graph[V, E]): GraphFunctions[V, E] =
    new GraphFunctions(graph)

}
