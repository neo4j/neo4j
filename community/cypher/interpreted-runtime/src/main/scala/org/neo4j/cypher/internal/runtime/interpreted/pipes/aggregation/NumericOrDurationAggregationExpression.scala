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

import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.NumberValue

trait NumericOrDurationAggregationExpression extends AggregationFunction {
  trait AggregatingType
  case object AggregatingNumbers extends AggregatingType
  case object AggregatingDurations extends AggregatingType

  protected var aggregatingType: Option[AggregatingType] = None

  def name: String

  def value: Expression

  protected def actOnNumberOrDuration(
    vl: AnyValue,
    aggNumber: NumberValue => Unit,
    aggDuration: DurationValue => Unit,
    state: QueryState
  ): Unit = {
    vl match {
      case IsNoValue() => onNoValue(state)
      case number: NumberValue =>
        aggregatingType match {
          case None =>
            aggregatingType = Some(AggregatingNumbers)
          case Some(AggregatingDurations) =>
            throw new CypherTypeException("%s(%s) cannot mix number and duration".format(name, value))
          case _ =>
        }
        aggNumber(number)
      case dur: DurationValue =>
        aggregatingType match {
          case None =>
            aggregatingType = Some(AggregatingDurations)
          case Some(AggregatingNumbers) =>
            throw new CypherTypeException("%s(%s) cannot mix number and duration".format(name, value))
          case _ =>
        }
        aggDuration(dur)
      case _ =>
        throw new CypherTypeException("%s(%s) can only handle numerical values, duration, or null.".format(name, value))
    }
  }
}
