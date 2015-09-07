
/** Copyright 2015, Metreta Information Technology s.r.l. */

package com.metreta.spark.orientdb.connector.rdd.partitioner

import org.apache.spark.Partition
import java.net.InetAddress

trait EndpointPartition extends Partition{
  def endpoints: Iterable[InetAddress]
}

case class OrientPartition(index: Int,
                              endpoints: Iterable[InetAddress],
                              partitionName: PartitionName) extends EndpointPartition
                              
case class PartitionName(className: String, clusterName: String)