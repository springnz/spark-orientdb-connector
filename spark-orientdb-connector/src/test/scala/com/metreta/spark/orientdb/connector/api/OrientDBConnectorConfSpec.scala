
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.api

import com.metreta.spark.orientdb.connector.utils.BaseOrientDbFlatSpec

class OrientDBConnectorConfSpec extends BaseOrientDbFlatSpec {

  it should "match a configuration with the same settings" in {
    val conf1 = OrientDBConnectorConf(defaultSparkConf)
    val conf2 = OrientDBConnectorConf(defaultSparkConf)
    conf1 should equal(conf2)
  }

}