package org.apache.spark.ae

import org.apache.spark.{MapOutputStatistics, SparkEnv}

import scala.collection.mutable.ArrayBuffer

object Utils {

//    val useAe = new org.apache.spark.internal.config.ConfigBuilder("spark.use.ae").booleanConf.createWithDefault(false);
//    val aeBlockSize = new org.apache.spark.internal.config.ConfigBuilder("spark.ae.block.size").longConf.createWithDefault(64 * 1024 * 1024);

  def estimatePartitionStartEndIndices(mapOutputStatisticsList : List[MapOutputStatistics]): Array[Int] = {
    val totalSize = mapOutputStatisticsList.map(_.bytesByPartitionId.sum).sum
    var minPartitonNum = SparkEnv.get.conf.getLong("spark.ae.min.partition.num", 1)
    var partitionSize = SparkEnv.get.conf.getLong("spark.ae.block.size", 64 * 1024 * 1024)
    var partitionRecordCount = SparkEnv.get.conf.getLong("spark.ae.record.count", 10000)

    partitionSize =
      math.min(math.ceil(totalSize / minPartitonNum.toDouble).toLong, partitionSize)

    val prePartitionNum = mapOutputStatisticsList.head.bytesByPartitionId.length

    val partitionIndex = ArrayBuffer[Int]()

    def partitionSizeAndRowCount(partitionId: Int): (Long, Long) = {
      var size = 0L
      var rowCount = 0L
      mapOutputStatisticsList.map{x =>
        size += x.bytesByPartitionId(partitionId)
        if (x.recordsByPartitionId.nonEmpty) {
          rowCount += x.recordsByPartitionId(partitionId)
        }
      }
//      println((size, rowCount))
      (size, rowCount)
    }

    var (postShuffleInputSize, postShuffleInputRowCount) = partitionSizeAndRowCount(0)
    var nextIndex = 1
    while (nextIndex < prePartitionNum) {
      val (nextShuffleInputSize, nextShuffleInputRowCount) = partitionSizeAndRowCount(nextIndex)
      if (postShuffleInputSize + nextShuffleInputSize > partitionSize
        || postShuffleInputRowCount + nextShuffleInputRowCount > partitionRecordCount) {
        partitionIndex += nextIndex
        postShuffleInputSize = nextShuffleInputSize
        postShuffleInputRowCount = nextShuffleInputRowCount
      } else {
        postShuffleInputSize += nextShuffleInputSize
        postShuffleInputRowCount += nextShuffleInputRowCount
      }
      nextIndex += 1
    }
    partitionIndex += nextIndex

    partitionIndex.toArray
  }
}