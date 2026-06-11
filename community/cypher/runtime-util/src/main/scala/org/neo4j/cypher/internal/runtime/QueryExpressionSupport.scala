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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner
import org.neo4j.cypher.internal.logical.plans.AllQueryExpression
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.EntityFilterQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRange
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.MinMaxOrdering
import org.neo4j.cypher.internal.logical.plans.NonExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.operations.CypherTypeValueMapper
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Shared [[QueryExpression]] → [[PropertyIndexQuery]] dispatch used by the interpreted/slotted/
 * non-fused pipelined runtimes.
 * The single entry point is:
 *
 *   - [[compile]] — compile-once dispatch. Walks `qe`; `prepareLeaf` runs once per leaf to amortize per-leaf
 *     work (e.g. parameter-name normalization) across all argumentIds in the SPD case.
 *     Returns a [[CompiledQueryExpression]] that, given a [[ValueResolver]] at
 *     apply time, produces the per-row `Seq[Seq[PropertyIndexQuery]]`.
 */
object QueryExpressionSupport {

  /**
   * Strategy supplied to a [[CompiledQueryExpression]] at apply time. Implementations override
   * `eval`; `one` is provided (`final`) so all resolvers share identical NO_VALUE handling.
   */
  trait ValueResolver[P] {
    def eval(prepared: P): AnyValue
    final def one(prepared: P): Option[Value] = makeValueNeoSafe.safeOrEmpty(eval(prepared))
  }

  /**
   * Output of [[compile]]. Holds a closure that produces the final `Seq[Seq[PropertyIndexQuery]]`
   * from a [[ValueResolver]]. Outer `Seq` = alternative sub-queries (unioned at query time);
   * inner `Seq` = conjunction of predicates across composite-index properties.
   */
  trait CompiledQueryExpression[P] {
    def apply(resolver: ValueResolver[P]): collection.Seq[collection.immutable.Seq[PropertyIndexQuery]]
  }

  private val SimpleName = getClass.getSimpleName

  private def fail(msg: String): Nothing =
    throw InternalException.internalError(SimpleName, msg)

  // ---- Public entry points ----

  /**
   * Compile-once dispatch. `prepareLeaf` runs once per leaf at plan time (amortizing leaf-level
   * work across all apply calls). For `RangeQueryExpression`, [[prepareExtractedRange]] walks the
   * range's nested leaves using [[InequalitySeekRange.mapBounds]] / `.map` so the returned
   * [[CompiledQueryExpression]] captures an [[ExtractedRange]][P] directly — no unsafe casts and
   * no intermediate [[QueryExpression]][P] tree.
   */
  def compile[E, P](
    qe: QueryExpression[E],
    propertyIds: Array[Int],
    prepareLeaf: E => P,
    rangeExtractor: RangeExtractor[E]
  ): CompiledQueryExpression[P] = {
    val factory: ValueResolver[P] => collection.Seq[collection.immutable.Seq[PropertyIndexQuery]] = qe match {
      case SingleQueryExpression(inner) =>
        val preparedLeaf: P = prepareLeaf(inner)
        (r: ValueResolver[P]) => dispatchSingle(preparedLeaf, propertyIds.head, r.one)

      case ManyQueryExpression(inner) =>
        val preparedLeaf: P = prepareLeaf(inner)
        (r: ValueResolver[P]) => dispatchMany(preparedLeaf, propertyIds.head, r.eval)

      case RangeQueryExpression(wrapper) =>
        checkOnlyWhenAssertionsAreEnabled(propertyIds.length == 1)
        val preparedRange: ExtractedRange[P] =
          prepareExtractedRange(rangeExtractor.extract(wrapper), prepareLeaf)
        (r: ValueResolver[P]) => dispatchRange(preparedRange, propertyIds.head, r.one)

      case CompositeQueryExpression(exprs) =>
        checkOnlyWhenAssertionsAreEnabled(exprs.lengthCompare(propertyIds.length) == 0)
        val memberFactories: collection.Seq[ValueResolver[P] => collection.Seq[PropertyIndexQuery]] =
          exprs.zip(propertyIds).map { case (e, pid) =>
            compileCompositeMember(e, pid, prepareLeaf, rangeExtractor)
          }
        (r: ValueResolver[P]) => SeqCombiner.combine(memberFactories.map(_.apply(r)))

      case ExistenceQueryExpression =>
        fail("An ExistenceQueryExpression shouldn't be found outside of a CompositeQueryExpression")

      case NonExistenceQueryExpression =>
        fail("A NonExistenceQueryExpression shouldn't be found outside of a Search Predicate")

      case AllQueryExpression =>
        fail(s"Unexpected value $qe")

      case _: EntityFilterQueryExpression[_] =>
        fail("An EntityFilterQueryExpression can only be planned for vector searches")
    }

    new CompiledQueryExpression[P] {
      override def apply(resolver: ValueResolver[P]): collection.Seq[collection.immutable.Seq[PropertyIndexQuery]] =
        factory(resolver)
    }
  }

  /**
   * Builds a kernel range [[PropertyIndexQuery]] from an already-resolved
   * [[InequalitySeekRange]][Value]. Public helper for callers that have already produced a
   * resolved `Option[InequalitySeekRange[Value]]` and only need the kernel-query construction.
   */
  def computeIndexRangeQuery(
    maybeSeekRange: Option[InequalitySeekRange[Value]],
    propertyId: Int
  ): Array[PropertyIndexQuery] =
    computeIndexRangeQueryArray(maybeSeekRange, propertyId)

  // ---- Private internals ----

  private def computeIndexRangeQueryArray(
    maybeSeekRange: Option[InequalitySeekRange[Value]],
    propertyId: Int
  ): Array[PropertyIndexQuery] = {
    maybeSeekRange match {
      case None => Array.empty
      case Some(valueRange) =>
        val groupedRanges = valueRange.groupBy(bound => bound.endPoint.valueGroup())
        if (groupedRanges.size > 1) {
          Array.empty // predicates of more than one value group mean that no node can ever match
        } else {
          val (_, range) = groupedRanges.head
          range match {
            case rangeLessThan: RangeLessThan[Value] =>
              rangeLessThan.limit(BY_VALUE).map(limit =>
                PropertyIndexQuery.range(propertyId, null, false, limit.endPoint, limit.isInclusive)
              ).toArray

            case rangeGreaterThan: RangeGreaterThan[Value] =>
              rangeGreaterThan.limit(BY_VALUE).map(limit =>
                PropertyIndexQuery.range(propertyId, limit.endPoint, limit.isInclusive, null, false)
              ).toArray

            case RangeBetween(rangeGreaterThan, rangeLessThan) =>
              val greaterThanLimit = rangeGreaterThan.limit(BY_VALUE).get
              val lessThanLimit = rangeLessThan.limit(BY_VALUE).get
              val compare = Values.COMPARATOR.compare(greaterThanLimit.endPoint, lessThanLimit.endPoint)
              if (compare < 0) {
                Array(PropertyIndexQuery.range(
                  propertyId,
                  greaterThanLimit.endPoint,
                  greaterThanLimit.isInclusive,
                  lessThanLimit.endPoint,
                  lessThanLimit.isInclusive
                ))
              } else if (compare == 0 && greaterThanLimit.isInclusive && lessThanLimit.isInclusive) {
                Array(PropertyIndexQuery.exact(propertyId, lessThanLimit.endPoint))
              } else {
                Array.empty
              }
          }
        }
    }
  }

  private def computeIndexRangeQueryNested(
    maybeSeekRange: Option[InequalitySeekRange[Value]],
    propertyId: Int
  ): collection.Seq[collection.immutable.Seq[PropertyIndexQuery]] = {
    val arr = computeIndexRangeQueryArray(maybeSeekRange, propertyId)
    if (arr.length == 0) Seq.empty
    else {
      val out = new Array[collection.immutable.Seq[PropertyIndexQuery]](arr.length)
      var i = 0
      while (i < arr.length) {
        out(i) = Seq(arr(i))
        i += 1
      }
      out
    }
  }

  private val BY_VALUE: MinMaxOrdering[Value] =
    MinMaxOrdering(Ordering.comparatorToOrdering(Values.COMPARATOR))

  // Top-level per-case helpers. Return `Seq[Seq[PropertyIndexQuery]]` where the outer `Seq`
  // enumerates alternative sub-queries (unioned)

  private def dispatchSingle[L](
    inner: L,
    propId: Int,
    oneLeaf: L => Option[Value]
  ): collection.Seq[collection.immutable.Seq[PropertyIndexQuery]] =
    oneLeaf(inner) match {
      case Some(seekValue) => Seq(Seq(PropertyIndexQuery.exact(propId, seekValue)))
      case None            => Seq.empty
    }

  private def dispatchMany[L](
    inner: L,
    propId: Int,
    evalLeaf: L => AnyValue
  ): collection.Seq[collection.immutable.Seq[PropertyIndexQuery]] =
    evalLeaf(inner) match {
      case IsList(coll) =>
        val flat = buildDedupedExactQueries(coll.asArray(), propId)
        val n = flat.size
        val out = new Array[collection.immutable.Seq[PropertyIndexQuery]](n)
        var i = 0
        while (i < n) {
          out(i) = List(flat(i))
          i += 1
        }
        out
      case v if v eq Values.NO_VALUE => Seq.empty
      case value: Value =>
        throw CypherTypeException.expectedList(
          String.valueOf(value),
          value.prettyPrint(),
          CypherTypeValueMapper.valueType(value)
        )
      case other =>
        throw CypherTypeException.expectedList(
          String.valueOf(other),
          String.valueOf(other),
          CypherTypeValueMapper.valueType(other)
        )
    }

  private def buildDedupedExactQueries(
    arr: Array[AnyValue],
    propId: Int
  ): collection.IndexedSeq[PropertyIndexQuery] = {
    val seen = new mutable.HashSet[AnyValue]
    seen.sizeHint(arr.length)
    val builder = mutable.ArrayBuilder.make[PropertyIndexQuery]
    builder.sizeHint(arr.length)
    var i = 0
    while (i < arr.length) {
      makeValueNeoSafe.safeOrEmpty(arr(i)).foreach { safeValue =>
        if (seen.add(safeValue)) {
          builder.addOne(PropertyIndexQuery.exact(propId, safeValue))
        }
      }
      i += 1
    }
    builder.result()
  }

  private def dispatchRange[L](
    extracted: ExtractedRange[L],
    propId: Int,
    oneLeaf: L => Option[Value]
  ): collection.Seq[collection.immutable.Seq[PropertyIndexQuery]] = extracted match {
    case ExtractedRange.Prefix(range) =>
      oneLeaf(range.prefix) match {
        case Some(text: TextValue) => Seq(Seq(PropertyIndexQuery.stringPrefix(propId, text)))
        case _                     => Seq.empty
      }

    case ExtractedRange.Inequality(range) =>
      computeIndexRangeQueryNested(range.flatMapBounds(oneLeaf), propId)

    case ExtractedRange.PointDistance(range) =>
      (oneLeaf(range.distance), oneLeaf(range.point)) match {
        case (Some(d: NumberValue), Some(p: PointValue)) =>
          val bboxes = p.getCoordinateReferenceSystem.getCalculator.boundingBox(p, d.doubleValue()).asScala
          // geographic calculator pads the range to avoid numerical errors, which means we rely
          // more on post-filtering. also means we can fix the date-line '<' case by simply
          // being inclusive in the index seek, and again rely on post-filtering.
          val n = bboxes.length
          val inclusive = if (n > 1) true else range.inclusive
          val out = new Array[collection.immutable.Seq[PropertyIndexQuery]](n)
          var i = 0
          while (i < n) {
            val b = bboxes(i)
            out(i) = Seq(PropertyIndexQuery.boundingBox(propId, b.first(), b.other(), inclusive))
            i += 1
          }
          out
        case _ => Seq.empty
      }

    case ExtractedRange.PointBoundingBox(range) =>
      (oneLeaf(range.lowerLeft), oneLeaf(range.upperRight)) match {
        case (Some(ll: PointValue), Some(ur: PointValue))
          if ll.getCoordinateReferenceSystem.equals(ur.getCoordinateReferenceSystem) =>
          val calc = ll.getCoordinateReferenceSystem.getCalculator
          val bboxes = calc.computeBBoxes(ll, ur).asScala
          val n = bboxes.length
          val out = new Array[collection.immutable.Seq[PropertyIndexQuery]](n)
          var i = 0
          while (i < n) {
            val b = bboxes(i)
            out(i) = Seq(PropertyIndexQuery.boundingBox(propId, b.first(), b.other()))
            i += 1
          }
          out
        case _ => Seq.empty
      }
  }

  private def compositeMemberSingle[L](
    leaf: L,
    propId: Int,
    oneLeaf: L => Option[Value]
  ): collection.Seq[PropertyIndexQuery] =
    dispatchSingle(leaf, propId, oneLeaf).flatten

  private def compositeMemberMany[L](
    leaf: L,
    propId: Int,
    evalLeaf: L => AnyValue
  ): collection.Seq[PropertyIndexQuery] = {
    evalLeaf(leaf) match {
      case IsList(coll) => buildDedupedExactQueries(coll.asArray(), propId)
      case null         => Seq.empty
      case value: Value =>
        throw CypherTypeException.expectedCollectionWasNot(
          String.valueOf(value),
          value.prettyPrint(),
          CypherTypeValueMapper.valueType(value)
        )
      case other =>
        throw CypherTypeException.expectedCollectionWasNot(
          String.valueOf(other),
          String.valueOf(other),
          CypherTypeValueMapper.valueType(other)
        )
    }
  }

  private def compositeMemberRange[L](
    extracted: ExtractedRange[L],
    propId: Int,
    oneLeaf: L => Option[Value]
  ): collection.Seq[PropertyIndexQuery] =
    dispatchRange(extracted, propId, oneLeaf).flatten

  /**
   * Plan-time transformation of a range's nested leaves from `E` to `P`. Uses the existing
   * type-class `map` / `mapBounds` methods on the concrete range types so the plan walker is
   * drift-resistant to future range-type field additions.
   */
  private def prepareExtractedRange[E, P](
    er: ExtractedRange[E],
    prepareLeaf: E => P
  ): ExtractedRange[P] = er match {
    case ExtractedRange.Prefix(r)           => ExtractedRange.Prefix(r.map(prepareLeaf))
    case ExtractedRange.Inequality(r)       => ExtractedRange.Inequality(r.mapBounds(prepareLeaf))
    case ExtractedRange.PointDistance(r)    => ExtractedRange.PointDistance(r.map(prepareLeaf))
    case ExtractedRange.PointBoundingBox(r) => ExtractedRange.PointBoundingBox(r.map(prepareLeaf))
  }

  /**
   * Plan-time composite-member factory builder. Walks one member's [[QueryExpression]] and
   * returns a `ValueResolver[P] => Seq[PropertyIndexQuery]` closure that calls the shared
   * `compositeMember*` helpers at apply time.
   */
  private def compileCompositeMember[E, P](
    qe: QueryExpression[E],
    propId: Int,
    prepareLeaf: E => P,
    rangeExtractor: RangeExtractor[E]
  ): ValueResolver[P] => collection.Seq[PropertyIndexQuery] = qe match {
    case SingleQueryExpression(inner) =>
      val preparedLeaf: P = prepareLeaf(inner)
      (r: ValueResolver[P]) => compositeMemberSingle(preparedLeaf, propId, r.one)

    case ManyQueryExpression(inner) =>
      val preparedLeaf: P = prepareLeaf(inner)
      (r: ValueResolver[P]) => compositeMemberMany(preparedLeaf, propId, r.eval)

    case RangeQueryExpression(wrapper) =>
      val preparedRange: ExtractedRange[P] =
        prepareExtractedRange(rangeExtractor.extract(wrapper), prepareLeaf)
      (r: ValueResolver[P]) => compositeMemberRange(preparedRange, propId, r.one)

    case ExistenceQueryExpression    => _ => Seq(PropertyIndexQuery.exists(propId))
    case NonExistenceQueryExpression => _ => Seq(PropertyIndexQuery.notExists(propId))
    case AllQueryExpression          => _ => Seq(PropertyIndexQuery.all(propId))

    case CompositeQueryExpression(_) =>
      fail("A CompositeQueryExpression can't be nested in a CompositeQueryExpression")

    case _: EntityFilterQueryExpression[_] =>
      fail("An EntityFilterQueryExpression can only be planned for vector searches")
  }
}
