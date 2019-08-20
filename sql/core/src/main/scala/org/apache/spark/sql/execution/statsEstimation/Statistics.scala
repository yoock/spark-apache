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

package org.apache.spark.sql.execution.statsEstimation

import java.math.{MathContext, RoundingMode}

import org.apache.spark.util.Utils


/**
 * Estimates of various statistics.  The default estimation logic simply lazily multiplies the
 * corresponding statistic produced by the children.  To override this behavior, override
 * `statistics` and assign it an overridden version of `Statistics`.
 *
 * '''NOTE''': concrete and/or overridden versions of statistics fields should pay attention to the
 * performance of the implementations.  The reason is that estimations might get triggered in
 * performance-critical processes, such as query plan planning.
 *
 * Note that we are using a BigInt here since it is easy to overflow a 64-bit integer in
 * cardinality estimation (e.g. cartesian joins).
 *
 * @param sizeInBytes Physical size in bytes. For leaf operators this defaults to 1, otherwise it
 *                    defaults to the product of children's `sizeInBytes`.
 * @param rowCount Estimated number of rows.
 */

case class PartitionStatistics(
    bytesByPartitionId: Array[Long],
    recordsByPartitionId: Array[Long])

case class RecordStatistics(
    record: BigInt,
    recordsByPartitionId: Array[Long])

case class Statistics(
    sizeInBytes: BigInt,
    bytesByPartitionId: Option[Array[Long]] = None,
    recordStatistics: Option[RecordStatistics] = None) {

  override def toString: String = "Statistics(" + simpleString + ")"

  /** Readable string representation for the Statistics. */
  def simpleString: String = {
    Seq(s"sizeInBytes=${Utils.bytesToString(sizeInBytes)}",
      if (recordStatistics.isDefined) {
        // Show row count in scientific notation.
        s"record=${BigDecimal(recordStatistics.get.record,
          new MathContext(3, RoundingMode.HALF_UP)).toString()}"
      } else {
        ""
      }
    ).filter(_.nonEmpty).mkString(", ")
  }

  def getPartitionStatistics : Option[PartitionStatistics] = {
    if (bytesByPartitionId.isDefined && recordStatistics.isDefined) {
      Some(PartitionStatistics(bytesByPartitionId.get, recordStatistics.get.recordsByPartitionId))
    } else {
      None
    }
  }
}
