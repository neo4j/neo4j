/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils
import org.neo4j.cypher.internal.runtime.vectorized.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.vectorized.operators.GroupingOffsets
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues.list
import org.neo4j.values.virtual.{ListValue, VirtualValues}

object  AggregationHelper {

  def groupingFunction(groupings: Array[GroupingOffsets]): (ExecutionContext, OldQueryState) => AnyValue = {
    groupings.length match {
      case 1 => (ctx, state) => groupings.head.expression(ctx, state)
      case 2 =>
        val e1 = groupings(0).expression
        val e2 = groupings(1).expression
        (ctx, state) => VirtualValues.list(e1(ctx, state), e2(ctx, state))
      case 3 =>
        val e1 = groupings(0).expression
        val e2 = groupings(1).expression
        val e3 = groupings(2).expression
        (ctx, state) => VirtualValues.list(e1(ctx, state), e2(ctx, state), e3(ctx, state))

      case _ =>
        val expressions = groupings.map(_.expression)
        (ctx, state) => VirtualValues.list(expressions.map(e => e(ctx, state)): _*)
    }
  }
  /**
    * Helper method to facilitate computing the correct setter method given a grouping at compile time
    */
  def computeGroupingSetter(groupings: Array[GroupingOffsets]): (MorselExecutionContext, AnyValue) => Unit = {
    val setInSlotFunctions = groupings.map(_.incoming).map(slot => SlottedPipeBuilderUtils.makeSetValueInSlotFunctionFor(slot))

    groupings.length match {
      case 1 => setInSlotFunctions(0)
      case 2 =>
        (ctx: MorselExecutionContext, key: AnyValue) => {
          val t2 = key.asInstanceOf[ListValue]
          setInSlotFunctions(0)(ctx, t2.head())
          setInSlotFunctions(1)(ctx, t2.last())
        }
      case 3 =>
        (ctx: MorselExecutionContext, groupingKey: AnyValue) => {
          val t3 = groupingKey.asInstanceOf[ListValue]
          setInSlotFunctions(0)(ctx, t3.value(0))
          setInSlotFunctions(1)(ctx, t3.value(1))
          setInSlotFunctions(2)(ctx, t3.value(2))
        }
      case _ =>
        (ctx: MorselExecutionContext, groupingKey: AnyValue) => {
          val listOfValues = groupingKey.asInstanceOf[ListValue]
          for (i <- groupings.indices) {
            val value: AnyValue = listOfValues.value(i)
            setInSlotFunctions(i)(ctx, value)
          }
        }
    }
  }

  /**
    * Helper method to facilitate computing the correct getter method given a grouping at compile time
    */
  def computeGroupingGetter(groupings: Array[GroupingOffsets]): (MorselExecutionContext) => AnyValue = {
    val getters = groupings.map(_.incoming).map(slot => SlottedPipeBuilderUtils.makeGetValueFromSlotFunctionFor(slot))
    groupings.length match {
      case 1 => getters(0)
      case 2 => (ctx: MorselExecutionContext) => list(getters(0)(ctx), getters(1)(ctx))
      case 3 => (ctx: MorselExecutionContext) => list(getters(0)(ctx), getters(1)(ctx), getters(2)(ctx))
      case _ => (ctx: MorselExecutionContext) => list(groupings.indices.map(i => getters(i)(ctx)):_*)
    }
  }
}
