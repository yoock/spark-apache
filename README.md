# Spark SQL Adaptive Execution
There are three main features in Adaptive Execution, including auto setting the shuffle partition number, optimi
An Engilish version design doc is available on [google doc](https://docs.google.com/document/d/1mpVjvQZRAkD-Ggy6
## Auto Setting The Shuffle Partition Number
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.adaptive.enabled</code></td>
  <td>false</td>
  <td>
    When true, enable adaptive query execution.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.minNumPostShufflePartitions</code></td>
  <td>1</td>
  <td>
    The minimum number of post-shuffle partitions used in adaptive execution. This can be used to control the mi
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.maxNumPostShufflePartitions</code></td>
  <td>500</td>
  <td>
    The maximum number of post-shuffle partitions used in adaptive execution. This is also used as the initial s
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.shuffle.targetPostShuffleInputSize</code></td>
  <td>67108864</td>
  <td>
    The target post-shuffle input size in bytes of a task. By default is 64 MB.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.shuffle.targetPostShuffleRowCount</code></td>
  <td>20000000</td>
  <td>
    The target post-shuffle row count of a task. This only takes effect if row count information is collected.
  </td>
</tr>
</table>

## Optimizing Join Strategy at Runtime
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.adaptive.join.enabled</code></td>
  <td>true</td>
  <td>
    When true and <code>spark.sql.adaptive.enabled</code> is enabled, a better join strategy is determined at ru
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptiveBroadcastJoinThreshold</code></td>
  <td>equals to <code>spark.sql.autoBroadcastJoinThreshold</code></td>
  <td>
    Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing 
  </td>
</tr>
</table>

## Handling Skewed Join
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.adaptive.skewedJoin.enabled</code></td>
  <td>false</td>
  <td>
    When true and <code>spark.sql.adaptive.enabled</code> is enabled, a skewed join is automatically handled at 
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.skewedPartitionFactor</code></td>
  <td>10</code></td>
  <td>
    A partition is considered as a skewed partition if its size is larger than this factor multiple the median p
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.skewedPartitionSizeThreshold</code></td>
  <td>67108864</td>
  <td>
    Configures the minimum size in bytes for a partition that is considered as a skewed partition in adaptive sk
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.skewedPartitionRowCountThreshold</code></td>
  <td>10000000</td>
  <td>
    Configures the minimum row count for a partition that is considered as a skewed partition in adaptive skewed
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.statistics.verbose</code></td>
  <td>false</td>
  <td>
    Collect shuffle statistics in verbose mode, including row counts etc. This is required for handling skewed j
  </td>
</tr>
</table>