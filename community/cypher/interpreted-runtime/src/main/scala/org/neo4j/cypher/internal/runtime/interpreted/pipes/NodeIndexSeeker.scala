/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.runtime.CompositeValueIndexCursor
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.InequalitySeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PointDistanceSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PrefixSeekRangeExpression
import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.IsList
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.v4_0.frontend.helpers.SeqCombiner.combine
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.IndexQuery
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.COMPARATOR
import org.neo4j.values.storable._

import scala.collection.JavaConverters._

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
  protected def indexSeek[RESULT <: AnyRef](state: QueryState,
                                            index: IndexReadSession,
                                            needsValues: Boolean,
                                            indexOrder: IndexOrder,
                                            baseContext: ExecutionContext): NodeValueIndexCursor =
    indexMode match {
      case _: ExactSeek |
           _: SeekByRange =>
        val indexQueries: Seq[Seq[IndexQuery]] = computeIndexQueries(state, baseContext)
        if (indexQueries.size == 1) {
          state.query.indexSeek(index, needsValues, indexOrder, indexQueries.head)
        } else {
          orderedCursor(indexOrder, indexQueries.map(query => state.query.indexSeek(index, needsValues = needsValues || indexOrder != IndexOrderNone, indexOrder, query)).toArray)
        }

      case LockingUniqueIndexSeek =>
        val indexQueries = computeExactQueries(state, baseContext)
        if (indexQueries.size == 1) {
          state.query.lockingUniqueIndexSeek(index.reference(), indexQueries.head)
        } else {
          orderedCursor(indexOrder, indexQueries.map(query => state.query.lockingUniqueIndexSeek(index.reference(), query)).toArray)
        }
    }

  // helpers

  private def orderedCursor(indexOrder: IndexOrder, cursors: Array[NodeValueIndexCursor]) = indexOrder match {
    case IndexOrderNone => CompositeValueIndexCursor.unordered(cursors)
    case IndexOrderAscending => CompositeValueIndexCursor.ascending(cursors)
    case IndexOrderDescending => CompositeValueIndexCursor.descending(cursors)
  }

  private val BY_VALUE: MinMaxOrdering[Value] = MinMaxOrdering(Ordering.comparatorToOrdering(COMPARATOR))

  protected def computeIndexQueries(state: QueryState, row: ExecutionContext): Seq[Seq[IndexQuery]] =
    valueExpr match {

      // Index range seek over range of values
      case RangeQueryExpression(rangeWrapper) =>
        assert(propertyIds.length == 1)
        computeRangeQueries(state, row, rangeWrapper, propertyIds.head).map(Seq(_))

      // Index composite seek over all values
      case CompositeQueryExpression(exprs) =>
        // ex:   x in [1] AND y in ["a", "b"] AND z > 3.0 AND exists(p)
        assert(exprs.lengthCompare(propertyIds.length) == 0)

        // indexQueries = [
        //                  [exact(1)],
        //                  [exact("a"), exact("b")],
        //                  [greaterThan(3.0)],
        //                  [exists(p)]
        //                ]
        val indexQueries = exprs.zip(propertyIds).map {
          case (expr, propId) =>
            computeCompositeQueries(state, row)(expr, propId)
        }

        // [
        //  [exact(1), exact("a"), greaterThan(3.0), exists(p)],
        //  [exact(1), exact("b"), greaterThan(3.0), exists(p)]
        // ]
        combine(indexQueries)

      case ExistenceQueryExpression() =>
        throw new InternalException("An ExistenceQueryExpression shouldn't be found outside of a CompositeQueryExpression")

      case _ =>
        computeExactQueries(state, row)
    }

  private def computeRangeQueries(state: QueryState, row: ExecutionContext, rangeWrapper: Expression, propertyId: Int): Seq[IndexQuery] = {
    rangeWrapper match {
      case PrefixSeekRangeExpression(range) =>
        val expr = range.prefix
        expr(row, state) match {
          case text: TextValue =>
            Array(IndexQuery.stringPrefix(propertyId, text))
          case IsNoValue() =>
            Nil
          case other =>
            throw new CypherTypeException("Expected TextValue, got " + other)
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
              rangeLessThan.limit(BY_VALUE).map(limit => IndexQuery.range(propertyId, null, false, limit.endPoint, limit.isInclusive)).toSeq

            case rangeGreaterThan: RangeGreaterThan[Value] =>
              rangeGreaterThan.limit(BY_VALUE).map(limit => IndexQuery.range(propertyId, limit.endPoint, limit.isInclusive, null, false)).toSeq

            case RangeBetween(rangeGreaterThan, rangeLessThan) =>
              val greaterThanLimit = rangeGreaterThan.limit(BY_VALUE).get
              val lessThanLimit = rangeLessThan.limit(BY_VALUE).get
              val compare = COMPARATOR.compare(greaterThanLimit.endPoint, lessThanLimit.endPoint)
              if (compare < 0) {
                    List(IndexQuery.range(propertyId,
                      greaterThanLimit.endPoint,
                      greaterThanLimit.isInclusive,
                      lessThanLimit.endPoint,
                      lessThanLimit.isInclusive))

              } else if (compare == 0 && greaterThanLimit.isInclusive && lessThanLimit.isInclusive) {
                List(IndexQuery.exact(propertyId, lessThanLimit.endPoint))
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
            // The geographic calculator pads the range to avoid numerical errors, which means we rely more on post-filtering
            // This also means we can fix the date-line '<' case by simply being inclusive in the index seek, and again rely on post-filtering
            val inclusive = if (bboxes.length > 1) true else range.inclusive
            bboxes.map(bbox => IndexQuery.range(propertyId,
              bbox.first(),
              inclusive,
              bbox.other(),
              inclusive
            ))
          case _ => Nil
        }
    }
  }

  protected def computeExactQueries(state: QueryState, row: ExecutionContext): Seq[Seq[IndexQuery.ExactPredicate]] =
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

          case v if v eq Values.NO_VALUE => Array[Seq[IndexQuery.ExactPredicate]]()
          case other => throw new CypherTypeException(s"Expected list, got $other")
        }

      // Index exact value seek on multiple values, making use of a composite index over all values
      //    eg:   x in [1] AND y in ["a", "b"] AND z in [3.0]
      // Should only get here from LockingUniqueIndexSeek
      case CompositeQueryExpression(exprs) =>
        assert(exprs.lengthCompare(propertyIds.length) == 0)

        // indexQueries = [[1], ["a", "b"], [3.0]]
        val indexQueries: Seq[Seq[IndexQuery.ExactPredicate]] = exprs.zip(propertyIds).map {
          case (expr, propId) =>
            computeCompositeQueries(state, row)(expr, propId).flatMap {
              case e: IndexQuery.ExactPredicate => Some(e)
              case _ => throw new InternalException("Expected only exact for LockingUniqueIndexSeek")
            }
        }

        // combined = [[1, "a", 3.0], [1, "b", 3.0]]
        combine(indexQueries)
    }

  private def computeCompositeQueries(state: QueryState, row: ExecutionContext)(queryExpression: QueryExpression[Expression], propertyId: Int): Seq[IndexQuery] =
    queryExpression match {
      case SingleQueryExpression(inner) =>
        Seq(IndexQuery.exact(propertyId, makeValueNeoSafe(inner(row, state))))

      case ManyQueryExpression(inner) =>
        val expr: Seq[AnyValue] = inner(row, state) match {
          case IsList(coll) => coll.asArray()
          case null => Seq.empty
          case _ => throw new CypherTypeException(s"Expected the value for $inner to be a collection but it was not.")
        }
        expr.map(e => IndexQuery.exact(propertyId, makeValueNeoSafe(e)))

      case CompositeQueryExpression(_) =>
        throw new InternalException("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")

      case RangeQueryExpression(rangeWrapper) =>
        computeRangeQueries(state, row, rangeWrapper, propertyId)

      case ExistenceQueryExpression() =>
        Seq(IndexQuery.exists(propertyId))
    }
}
