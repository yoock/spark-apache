/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.adaptive

import org.apache.spark._
import org.apache.spark.rdd.{RDD, ShuffledRDDPartition}
import org.apache.spark.sql.catalyst.InternalRow

/**
 * This is a specialized version of [[org.apache.spark.sql.execution.ShuffledRowRDD]]. This is used
 * in Spark SQL adaptive execution when a shuffle join is converted to broadcast join at runtime
 * because the map output of one input table is small enough for broadcast. This RDD represents the
 * data of another input table of the join that reads from shuffle. Each partition of the RDD reads
 * the whole data from just one mapper output locally. So actually there is no data transferred
 * from the network.
 *
 * This RDD takes a [[ShuffleDependency]] (`dependency`).
 *
 * The `dependency` has the parent RDD of this RDD, which represents the dataset before shuffle
 * (i.e. map output). Elements of this RDD are (partitionId, Row) pairs.
 * Partition ids should be in the range [0, numPartitions - 1].
 * `dependency.partitioner.numPartitions` is the number of pre-shuffle partitions. (i.e. the number
 * of partitions of the map output). The post-shuffle partition number is the same to the parent
 * RDD's partition number.
 */
class LocalShuffledRowRDD(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
    specifiedPartitionStartIndices: Option[Array[Int]] = None,
    specifiedPartitionEndIndices: Option[Array[Int]] = None)
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  private[this] val numPreShufflePartitions = dependency.partitioner.numPartitions
  private[this] val numPostShufflePartitions = dependency.rdd.partitions.length

  private[this] val partitionStartIndices: Array[Int] = specifiedPartitionStartIndices match {
    case Some(indices) => indices
    case None => Array(0)
  }

  private[this] val partitionEndIndices: Array[Int] = specifiedPartitionEndIndices match {
    case Some(indices) => indices
    case None if specifiedPartitionStartIndices.isEmpty => Array(numPreShufflePartitions)
    case _ => specifiedPartitionStartIndices.get.drop(1) :+ numPreShufflePartitions
  }

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override def getPartitions: Array[Partition] = {
    assert(partitionStartIndices.length == partitionEndIndices.length)
    Array.tabulate[Partition](numPostShufflePartitions) { i =>
      new ShuffledRDDPartition(i)
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    tracker.getMapLocation(dep, partition.index, partition.index + 1).toSeq
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val shuffledRowPartition = split.asInstanceOf[ShuffledRDDPartition]
    val mapId = shuffledRowPartition.index
    // Connect the the InternalRows read by each ShuffleReader
    new Iterator[InternalRow] {
      val readers = partitionStartIndices.zip(partitionEndIndices).map { case (start, end) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          start,
          end,
          context,
          mapId,
          mapId + 1)
      }

      var i = 0
      var iter = readers(i).read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)

      override def hasNext = {
        while (iter.hasNext == false && i + 1 <= readers.length - 1) {
          i += 1
          iter = readers(i).read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
        }
        iter.hasNext
      }

      override def next() = iter.next()
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}
