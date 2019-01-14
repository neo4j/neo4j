/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.frontend.v3_4.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, InequalitySeekRangeExpression, PointDistanceSeekRangeExpression, PrefixSeekRangeExpression}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, IsList, makeValueNeoSafe}
import org.neo4j.cypher.internal.util.v3_4.{CypherTypeException, InternalException}
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.internal.kernel.api.{IndexQuery, IndexReference}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.NodeValue

import collection.JavaConverters._

/**
  * Mixin trait with functionality for executing logical index queries.
  *
  * This trait maps the logical IndexSeekMode and QueryExpression into the kernel IndexQuery classes, which
  * are passed to the QueryContext for executing the index seek.
  */
trait NodeIndexSeeker {

  // dependencies

  def indexMode: IndexSeekMode
  def valueExpr: QueryExpression[Expression]
  def propertyIds: Array[Int]

  // index seek

  protected def indexSeek(state: QueryState,
                          indexReference: IndexReference,
                          baseContext: ExecutionContext): Iterator[NodeValue] =
    indexMode match {
      case _: ExactSeek |
           _: SeekByRange =>
        val indexQueries = computeIndexQueries(state, baseContext)
        indexQueries.toIterator.flatMap(query => state.query.indexSeek(indexReference, query))

      case LockingUniqueIndexSeek =>
        val indexQueries = computeExactQueries(state, baseContext)
        indexQueries.flatMap(indexQuery => state.query.lockingUniqueIndexSeek(indexReference, indexQuery)).toIterator
    }

  // helpers

  private val BY_VALUE: MinMaxOrdering[Value] = MinMaxOrdering(Ordering.comparatorToOrdering(Values.COMPARATOR))

  private def computeIndexQueries(state: QueryState, row: ExecutionContext): Seq[Seq[IndexQuery]] =
    valueExpr match {

      // Index range seek over range of values
      case RangeQueryExpression(rangeWrapper) =>
        assert(propertyIds.length == 1)
        rangeWrapper match {
          case PrefixSeekRangeExpression(range) =>
            val expr = range.prefix
            expr(row, state) match {
              case text: TextValue =>
                Array(Seq(IndexQuery.stringPrefix(propertyIds.head, text.stringValue())))
              case Values.NO_VALUE =>
                Nil
              case other =>
                throw new CypherTypeException("Expected TextValue, got "+other )
            }

          case InequalitySeekRangeExpression(innerRange) =>
            val valueRange: InequalitySeekRange[Value] = innerRange.mapBounds(expr => makeValueNeoSafe(expr(row, state)))
            val groupedRanges = valueRange.groupBy(bound => bound.endPoint.valueGroup())
            if (groupedRanges.size > 1) {
              Nil // predicates of more than one value group mean that no node can ever match
            } else {
              val (valueGroup, range) = groupedRanges.head
              range match {
                case rangeLessThan: RangeLessThan[Value] =>
                  rangeLessThan.limit(BY_VALUE).map( limit =>
                    List(IndexQuery.range(propertyIds.head, null, false, limit.endPoint, limit.isInclusive))
                  ).toSeq

                case rangeGreaterThan: RangeGreaterThan[Value] =>
                  rangeGreaterThan.limit(BY_VALUE).map( limit =>
                    List(IndexQuery.range(propertyIds.head, limit.endPoint, limit.isInclusive, null, false))
                  ).toSeq

                case RangeBetween(rangeGreaterThan, rangeLessThan) =>
                  val greaterThanLimit = rangeGreaterThan.limit(BY_VALUE).get
                  val lessThanLimit = rangeLessThan.limit(BY_VALUE).get

                  val compare = Values.COMPARATOR.compare(greaterThanLimit.endPoint, lessThanLimit.endPoint)
                  if (compare < 0) {
                    List(List(IndexQuery.range(propertyIds.head,
                                         greaterThanLimit.endPoint,
                                         greaterThanLimit.isInclusive,
                                         lessThanLimit.endPoint,
                                         lessThanLimit.isInclusive)))
                  } else if (compare == 0 && greaterThanLimit.isInclusive && lessThanLimit.isInclusive) {
                    List(List(IndexQuery.exact(propertyIds.head, lessThanLimit.endPoint)))
                  } else {
                    Nil
                  }
              }
            }

          case PointDistanceSeekRangeExpression(range) =>
            val valueRange = range.map(expr => makeValueNeoSafe(expr(row, state)))
            (valueRange.distance, valueRange.point) match {
              case (distance: NumberValue, point: PointValue) =>
                val bboxes = point.getCoordinateReferenceSystem.getCalculator.boundingBox(point, distance.doubleValue()).asScala
                bboxes.map( bbox => List(IndexQuery.range(propertyIds.head,
                  bbox.first(),
                  range.inclusive,
                  bbox.other(),
                  range.inclusive
                )))
              case _ => Nil
            }
        }

      case exactQuery =>
        computeExactQueries(state, row)
    }

  private def computeExactQueries(state: QueryState, row: ExecutionContext): Seq[Seq[IndexQuery.ExactPredicate]] =
    valueExpr match {
      // Index exact value seek on single value
      case SingleQueryExpression(expr) =>
        val seekValue = makeValueNeoSafe(expr(row, state))
        Array(List(IndexQuery.exact(propertyIds.head, seekValue)))

      // Index exact value seek on multiple values, by combining the results of multiple index seeks
      case ManyQueryExpression(expr) =>
        expr(row, state) match {
          case IsList(coll) =>
            coll.asArray().toSet[AnyValue].map(
              seekAnyValue =>
                List(IndexQuery.exact(
                  propertyIds.head,
                  makeValueNeoSafe(seekAnyValue)
                ))
            ).toSeq

          case v if v == Values.NO_VALUE => Array[Seq[IndexQuery.ExactPredicate]]()
          case other => throw new CypherTypeException(s"Expected list, got $other")
        }

      // Index exact value seek on multiple values, making use of a composite index over all values
      //    eg:   x in [1] AND y in ["a", "b"] AND z in [3.0]
      case CompositeQueryExpression(exprs) =>
        assert(exprs.lengthCompare(propertyIds.length) == 0)

        // seekValues = [[1], ["a", "b"], [3.0]]
        val seekValues = exprs.map(expressionValues(row, state))

        // combined = [[1, "a", 3.0], [1, "b", 3.0]]
        val combined = combine(seekValues)
        combined.map(seekTuple => seekTuple.zip(propertyIds)
          .map { case (v,propId) => IndexQuery.exact(propId, makeValueNeoSafe(v))}
        )
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
