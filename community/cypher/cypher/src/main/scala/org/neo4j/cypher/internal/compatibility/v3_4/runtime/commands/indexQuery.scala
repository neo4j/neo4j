/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.{Expression, InequalitySeekRangeExpression, PrefixSeekRangeExpression}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.IsList
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.mutation.{GraphElementPropertyFunctions, makeValueNeoSafe}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_4.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.frontend.v3_4.{CypherTypeException, InternalException}
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.graphdb.Node
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._

object indexQuery extends GraphElementPropertyFunctions {
  def apply(queryExpression: QueryExpression[Expression],
            m: ExecutionContext,
            state: QueryState,
            index: Seq[Any] => GenTraversableOnce[Node],
            labelName: String,
            propertyNames: Seq[String]): Iterator[Node] = queryExpression match {

    // Index exact value seek on single value
    case SingleQueryExpression(inner) =>
      val value: AnyValue = inner(m, state)
      lookupNodes(Seq(value), index)

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
        new IteratorOfIterarors[Node](results)

    // Index range seek over range of values
    case RangeQueryExpression(rangeWrapper) =>
      val range = rangeWrapper match {
        case s: PrefixSeekRangeExpression =>
          s.range.map(expression => makeValueNeoSafe(expression(m, state)).asObject())

        case InequalitySeekRangeExpression(innerRange) =>
          innerRange.mapBounds(expression => makeValueNeoSafe(expression(m, state)).asObject())
      }
      index(Seq(range)).toIterator
  }

  private def lookupNodes(values: Seq[AnyValue], index: Seq[Any] => GenTraversableOnce[Node]): Iterator[Node] = {
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

class IteratorOfIterarors[T](inner: Seq[Iterator[T]]) extends Iterator[T] {
  private var _buffer: Option[T] = None
  private var _innerIterator = inner.toIterator
  private var _current: Iterator[T] = Iterator.empty

  fillBuffer()

  override def hasNext: Boolean = _buffer.nonEmpty

  override def next(): T = {
    if (isEmpty) Iterator.empty.next()

    val res = _buffer.get
    fillBuffer()
    res
  }

  private def fillBuffer(): Unit = {

    while (_current.isEmpty && _innerIterator.nonEmpty) {
      _current = _innerIterator.next()
    }

    if (_current.isEmpty)
      _buffer = None
    else
      _buffer = Some(_current.next())
  }
}
