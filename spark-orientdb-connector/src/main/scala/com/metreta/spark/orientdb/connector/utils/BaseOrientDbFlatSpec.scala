
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.utils

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.Suite

/**
 * Utility trait for Unit Test.
 */
trait BaseOrientDbFlatSpec extends FlatSpec with Suite with Matchers with SparkContextUtils with BeforeAndAfterAll
