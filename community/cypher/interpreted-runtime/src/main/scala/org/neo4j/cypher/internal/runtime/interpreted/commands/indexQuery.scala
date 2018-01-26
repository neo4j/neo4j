/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.frontend.v3_4.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, InequalitySeekRangeExpression, PrefixSeekRangeExpression}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.util.v3_4.{CypherTypeException, InternalException}
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{PointValue, Values}
import org.neo4j.values.virtual.NodeValue

import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._

object indexQuery extends GraphElementPropertyFunctions {
  def apply(queryExpression: QueryExpression[Expression],
            m: ExecutionContext,
            state: QueryState,
            index: Seq[Any] => GenTraversableOnce[NodeValue],
            labelName: String,
            propertyNames: Seq[String]): Iterator[NodeValue] = queryExpression match {

    // Index exact value seek on single value
    case SingleQueryExpression(inner) =>
      assert(1 == propertyNames.size)
      val value: AnyValue = inner(m, state)
      val indexValues = lookupNodes(Seq(value), index)
      value match {
        case _: PointValue =>
          val propertyKeyId = state.query.getPropertyKeyId(propertyNames.head)
          indexValues.filter { nv =>
            val propertyValue = state.query.nodeOps.getProperty(nv.id(), propertyKeyId)
            value.equals(propertyValue)
          }
        case _ =>
          indexValues
      }

    // Index exact value seek on multiple values, by combining the results of multiple index seeks
    case ManyQueryExpression(inner) =>
      inner(m, state) match {
        case IsList(coll) => coll.iterator().asScala.toSet.toIndexedSeq.flatMap {
          value: AnyValue => lookupNodes(Seq(value), index)
        }.iterator
        case v if v == Values.NO_VALUE => Iterator.empty
        case _ => throw new CypherTypeException(s"Expected the value for looking up :$labelName(${propertyNames.mkString(",")}) to be a collection but it was not.")
      }

    // Index exact value seek on multiple values, making use of a composite index over all values
    case CompositeQueryExpression(innerExpressions) =>
      assert(innerExpressions.size == propertyNames.size)
      val seekValues = innerExpressions.map(expressionValues(m, state))
      val combined = combine(seekValues)
      val results = combined map { values =>
        lookupNodes(values, index)
      }
      if (results.size == 1)
        results.head
      else
        results.iterator.flatten

    // Index range seek over range of values
    case RangeQueryExpression(rangeWrapper) =>
      val range = rangeWrapper match {
        case s: PrefixSeekRangeExpression =>
          s.range.map(expression => makeValueNeoSafe(expression(m, state)).asObject())

        case InequalitySeekRangeExpression(innerRange) =>
          innerRange.mapBounds(expression => makeValueNeoSafe(expression(m, state)).asObject())
      }
      range match {
        case r:InequalitySeekRange[_] if r.existsBound { value => value.isInstanceOf[PointValue] } =>
          val correctlyTypedRange = r.asInstanceOf[InequalitySeekRange[PointValue]]
          val propertyKeyId = state.query.getPropertyKeyId(propertyNames.head)
          index(Seq(range)).toIterator.filter { nv =>
            val propertyValue = state.query.nodeOps.getProperty(nv.id(), propertyKeyId).asInstanceOf[PointValue]
            correctlyTypedRange.includes(propertyValue)(CypherOrdering.BY_POINT)
          }
        case _ =>
          index(Seq(range)).toIterator
      }
  }

  private def lookupNodes(values: Seq[AnyValue], index: Seq[Any] => GenTraversableOnce[NodeValue]): Iterator[NodeValue] = {
    // If any of the values we are searching for is null, the whole expression that this index seek represents
    // collapses into a null value, which will not match any nodes.
    if (values.contains(Values.NO_VALUE))
      Iterator.empty
    else {
      val neoValues = values.map(makeValueNeoSafe).map(_.asObject())
      index(neoValues).toIterator
    }
  }

  private def expressionValues(m: ExecutionContext, state: QueryState)(queryExpression: QueryExpression[Expression]): Seq[AnyValue] = {
    queryExpression match {

      case SingleQueryExpression(inner) =>
        Seq(inner(m, state))

      case ManyQueryExpression(inner) =>
        inner(m, state) match {
          case IsList(coll) => coll.asArray()
          case null => Seq.empty
          case _ => throw new CypherTypeException(s"Expected the value for $inner to be a collection but it was not.")
        }

      case CompositeQueryExpression(innerExpressions) =>
        throw new InternalException("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")

      case RangeQueryExpression(rangeWrapper) =>
        throw new InternalException("Range queries on composite indexes not yet supported")
    }
  }
}


