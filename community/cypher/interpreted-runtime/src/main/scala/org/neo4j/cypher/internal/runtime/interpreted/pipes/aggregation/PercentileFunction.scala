/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.SequenceValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValueBuilder

abstract class PercentileFunction(val value: Expression, percentiles: Expression, memoryTracker: MemoryTracker)
    extends AggregationFunction
    with NumericExpressionOnly {

  protected var temp: HeapTrackingArrayList[NumberValue] =
    HeapTrackingCollections.newArrayList[NumberValue](memoryTracker)
  protected var count: Int = 0
  protected var percs: Array[Double] = _
  protected var estimatedNumberValue: Long = -1

  override def apply(data: ReadableRow, state: QueryState): Unit = {
    actOnNumber(
      value(data, state),
      number => {
        if (count < 1) {
          onFirstRow(data, state)
        }
        count += 1
        temp.add(number)
        if (estimatedNumberValue == -1) {
          estimatedNumberValue = number.estimatedHeapUsage();
        }
        memoryTracker.allocateHeap(estimatedNumberValue)
      }
    )
  }

  protected def onFirstRow(data: ReadableRow, state: QueryState): Unit = {
    percentiles(data, state) match {
      case percentilesValue: SequenceValue =>
        percs = new Array[Double](percentilesValue.length())
        var i = 0
        while (i < percentilesValue.length()) {
          val perc = NumericHelper.asDouble(percentilesValue.value(i)).doubleValue()
          percs(i) = perc
          if (perc < 0 || perc > 1.0)
            throw new InvalidArgumentException(
              s"Invalid input '$perc' is not a valid argument, must be a number in the range 0.0 to 1.0"
            )
          i += 1
        }
      case value =>
        percs = Array(NumericHelper.asDouble(value).doubleValue())
        if (percs(0) < 0 || percs(0) > 1.0)
          throw new InvalidArgumentException(
            s"Invalid input '${percs(0)}' is not a valid argument, must be a number in the range 0.0 to 1.0"
          )
    }
  }
}

class PercentileContFunction(value: Expression, percentile: Expression, memoryTracker: MemoryTracker)
    extends PercentileFunction(value, percentile, memoryTracker) {

  def name = "PERCENTILE_CONT"

  override def result(state: QueryState): AnyValue = {
    temp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))

    val result =
      if (count == 0) {
        Values.NO_VALUE
      } else {
        val perc = percs(0)
        if (perc == 1.0 || count == 1) {
          temp.get(count - 1)
        } else {
          val floatIdx = perc * (count - 1)
          val floor = floatIdx.toInt
          val ceil = math.ceil(floatIdx).toInt
          if (ceil == floor || floor == count - 1) temp.get(floor)
          else Values.doubleValue(NumericHelper.asDouble(temp.get(floor)).doubleValue() * (ceil - floatIdx) +
            NumericHelper.asDouble(temp.get(ceil)).doubleValue() * (floatIdx - floor))
        }
      }

    temp.close()
    temp = null
    memoryTracker.releaseHeap(count * estimatedNumberValue)
    result
  }
}

object PercentileContFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[PercentileContFunction])
}

class PercentileDiscFunction(value: Expression, percentile: Expression, memoryTracker: MemoryTracker)
    extends PercentileFunction(value, percentile, memoryTracker) {

  def name = "PERCENTILE_DISC"

  override def result(state: QueryState): AnyValue = {
    temp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))

    val result =
      if (count == 0) {
        Values.NO_VALUE
      } else {
        val perc = percs(0)
        if (perc == 1.0 || count == 1) {
          temp.get(count - 1)
        } else {
          val floatIdx = perc * count
          var idx = floatIdx.toInt
          idx =
            if (floatIdx != idx || idx == 0) idx
            else idx - 1
          temp.get(idx)
        }
      }

    temp.close()
    temp = null
    memoryTracker.releaseHeap(count * estimatedNumberValue)
    result
  }
}

object PercentileDiscFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[PercentileDiscFunction])
}

class MultiPercentileDiscFunction(
  value: Expression,
  percentiles: Expression,
  keys: Expression,
  memoryTracker: MemoryTracker
) extends PercentileFunction(value, percentiles, memoryTracker) {

  private var mapKeys: Array[String] = _
  override val name = "MULTI_PERCENTILE_DISC"

  override protected def onFirstRow(data: ReadableRow, state: QueryState): Unit = {
    super.onFirstRow(data, state)
    keys(data, state) match {
      case keysValue: SequenceValue =>
        mapKeys = new Array[String](keysValue.length())
        var i = 0
        while (i < mapKeys.length) {
          mapKeys(i) = CypherFunctions.asTextValue(keysValue.value(i)).stringValue()
          i += 1
        }
        if (keysValue.length() != percs.length) {
          throw new InternalException(
            s"Expected 'percentiles' ${percs.mkString(",")} and 'keys' ${mapKeys.mkString(",")} to have the same length"
          )
        }
    }
  }

  override def result(state: QueryState): AnyValue = {
    temp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))

    val result = {
      if (count == 0) {
        Values.NO_VALUE
      } else {
        val mapBuilder = new MapValueBuilder(percs.length)
        var i = 0
        while (i < percs.length) {
          val perc = percs(i)
          val mapKey = mapKeys(i)
          val percValue =
            if (perc == 1.0 || count == 1) {
              temp.get(count - 1)
            } else {
              val floatIdx = perc * count
              var idx = floatIdx.toInt
              idx =
                if (floatIdx != idx || idx == 0) idx
                else idx - 1
              temp.get(idx)
            }
          mapBuilder.add(mapKey, percValue)
          i += 1
        }
        mapBuilder.build()
      }
    }

    temp.close()
    temp = null
    memoryTracker.releaseHeap(count * estimatedNumberValue)
    result
  }
}

object MultiPercentileDiscFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[MultiPercentileDiscFunction])
}
