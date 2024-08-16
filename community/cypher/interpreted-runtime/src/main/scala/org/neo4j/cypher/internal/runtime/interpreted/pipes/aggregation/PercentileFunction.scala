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
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentDesc
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentOrder
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentUnordered
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherCoercions
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValueBuilder

abstract class PercentileFunction(val value: Expression, memoryTracker: MemoryTracker)
    extends NumericExpressionOnly
    with InitiateOnFirstRow {

  protected var temp: HeapTrackingArrayList[NumberValue] =
    HeapTrackingCollections.newArrayList[NumberValue](memoryTracker)
  protected var count: Int = 0
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
          estimatedNumberValue = number.estimatedHeapUsage()
        }
        memoryTracker.allocateHeap(estimatedNumberValue)
      },
      state
    )
  }
}

sealed trait InitiateOnFirstRow {
  protected def onFirstRow(data: ReadableRow, state: QueryState): Unit
}

trait OnePercentile extends InitiateOnFirstRow {
  protected var perc: Double = 0

  protected def percentile: Expression

  override protected def onFirstRow(data: ReadableRow, state: QueryState): Unit = {
    perc = CypherCoercions.asNumberValue(percentile(data, state)).doubleValue()
    if (perc < 0 || perc > 1.0)
      throw new InvalidArgumentException(
        s"Invalid input '$perc' is not a valid argument, must be a number in the range 0.0 to 1.0"
      )
  }
}

class PercentileContFunction(
  value: Expression,
  val percentile: Expression,
  memoryTracker: MemoryTracker,
  order: ArgumentOrder
) extends PercentileFunction(value, memoryTracker) with OnePercentile {

  def name = "PERCENTILE_CONT"

  override def result(state: QueryState): AnyValue = {
    if (order == ArgumentUnordered) {
      temp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))
    }

    val result =
      if (count == 0) {
        Values.NO_VALUE
      } else {
        PercentileContFunction.computePercentileCont(temp, count, perc, order)
      }

    temp.close()
    temp = null
    memoryTracker.releaseHeap(count * estimatedNumberValue)
    result
  }
}

object PercentileContFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[PercentileContFunction])

  def computePercentileCont(
    data: HeapTrackingArrayList[NumberValue],
    count: Int,
    percentile: Double,
    order: ArgumentOrder
  ): NumberValue = {
    if (percentile == 1.0 || count == 1) {
      if (order == ArgumentDesc) data.get(0) else data.get(count - 1)
    } else {
      val floatIdx = percentile * (count - 1)
      val floor = floatIdx.toInt
      val ceil = math.ceil(floatIdx).toInt
      if (order == ArgumentDesc) {
        if (ceil == floor || floor == count - 1) data.get(count - 1 - floor)
        else data.get(count - 1 - floor).times(ceil - floatIdx)
          .plus(data.get(count - 1 - ceil).times(floatIdx - floor))
      } else {
        if (ceil == floor || floor == count - 1) data.get(floor)
        else data.get(floor).times(ceil - floatIdx)
          .plus(data.get(ceil).times(floatIdx - floor))
      }
    }
  }
}

class PercentileDiscFunction(
  value: Expression,
  val percentile: Expression,
  memoryTracker: MemoryTracker,
  order: ArgumentOrder
) extends PercentileFunction(value, memoryTracker) with OnePercentile {

  def name = "PERCENTILE_DISC"

  override def result(state: QueryState): AnyValue = {
    if (order == ArgumentUnordered) {
      temp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))
    }

    val result =
      if (count == 0) {
        Values.NO_VALUE
      } else {
        PercentileDiscFunction.computePercentileDisc(temp, count, perc, order)
      }

    temp.close()
    temp = null
    memoryTracker.releaseHeap(count * estimatedNumberValue)
    result
  }
}

object PercentileDiscFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[PercentileDiscFunction])

  def computePercentileDisc(
    data: HeapTrackingArrayList[NumberValue],
    count: Int,
    percentile: Double,
    order: ArgumentOrder
  ): NumberValue = {
    if (percentile == 1.0 || count == 1) {
      if (order == ArgumentDesc) data.get(0) else data.get(count - 1)
    } else {
      val floatIdx = percentile * count
      val toInt = floatIdx.toInt
      val idx = if (floatIdx != toInt || toInt == 0) toInt else toInt - 1
      if (order == ArgumentDesc) {
        data.get(count - 1 - idx)
      } else {
        data.get(idx)
      }
    }
  }
}

class PercentilesFunction(
  value: Expression,
  percentiles: Expression,
  keys: Expression,
  isDiscreteRange: Expression,
  memoryTracker: MemoryTracker,
  order: ArgumentOrder
) extends PercentileFunction(value, memoryTracker) {

  private var percs: Array[Double] = _
  private var mapKeys: Array[String] = _
  private var isDiscretes: Array[Boolean] = _
  override val name = "PERCENTILES"

  override protected def onFirstRow(data: ReadableRow, state: QueryState): Unit = {
    val percsValue = CypherCoercions.asSequenceValue(percentiles(data, state))
    percs = new Array[Double](percsValue.intSize())
    var i = 0
    while (i < percsValue.intSize()) {
      val perc = CypherCoercions.asNumberValue(percsValue.value(i)).doubleValue()
      percs(i) = perc
      if (perc < 0 || perc > 1.0)
        throw new InvalidArgumentException(
          s"Invalid input '$perc' is not a valid argument, must be a number in the range 0.0 to 1.0"
        )
      i += 1
    }

    val keysValue = CypherCoercions.asSequenceValue(keys(data, state))
    mapKeys = new Array[String](keysValue.intSize())
    i = 0
    while (i < mapKeys.length) {
      mapKeys(i) = CypherFunctions.asTextValue(keysValue.value(i)).stringValue()
      i += 1
    }
    if (keysValue.intSize() != percs.length) {
      throw new InternalException(
        s"Expected 'percentiles' ${percs.mkString(",")} and 'keys' ${mapKeys.mkString(",")} to have the same length"
      )
    }

    val isDiscreteValues = CypherCoercions.asSequenceValue(isDiscreteRange(data, state))
    isDiscretes = new Array[Boolean](isDiscreteValues.intSize())
    i = 0
    while (i < isDiscretes.length) {
      isDiscretes(i) = isDiscreteValues.value(i).asInstanceOf[BooleanValue].booleanValue()
      i += 1
    }
    if (isDiscreteValues.intSize() != percs.length) {
      throw new InternalException(
        s"Expected 'percentiles' ${percs.mkString(",")} and 'isDiscreteRange' ${isDiscretes.mkString(",")} to have the same length"
      )
    }
  }

  override def result(state: QueryState): AnyValue = {
    if (order == ArgumentUnordered) {
      temp.sort((o1: NumberValue, o2: NumberValue) => java.lang.Double.compare(o1.doubleValue(), o2.doubleValue()))
    }

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
            if (isDiscretes(i)) {
              PercentileDiscFunction.computePercentileDisc(temp, count, perc, order)
            } else {
              PercentileContFunction.computePercentileCont(temp, count, perc, order)
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

object PercentilesFunction {
  val SHALLOW_SIZE: Long = shallowSizeOfInstance(classOf[PercentilesFunction])
}
