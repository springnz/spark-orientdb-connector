
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.reflect.ClassTag


abstract class  OrientRDD[R : ClassTag](
    sc: SparkContext,
    dep: Seq[Dependency[_]])
  extends RDD[R](sc, dep) {

}
