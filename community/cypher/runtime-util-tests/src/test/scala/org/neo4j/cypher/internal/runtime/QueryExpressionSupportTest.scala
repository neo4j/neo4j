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

import org.neo4j.cypher.internal.logical.plans.AllQueryExpression
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.InclusiveBound
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRange
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.MatchAllQueryExpression
import org.neo4j.cypher.internal.logical.plans.NonExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.runtime.QueryExpressionSupport.CompiledQueryExpression
import org.neo4j.cypher.internal.runtime.QueryExpressionSupport.ValueResolver
import org.neo4j.cypher.internal.runtime.RuntimeUtilTestSuite
import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.PropertyIndexQuery
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import java.util.concurrent.atomic.AtomicInteger

class QueryExpressionSupportTest extends RuntimeUtilTestSuite {

  // ----- Test leaf ADT + extractor + resolver ------------------------------

  sealed private trait TestLeaf
  private case class ValueLeaf(v: AnyValue) extends TestLeaf
  private case class RangeLeaf(wrapper: TestRangeWrapper) extends TestLeaf

  sealed private trait TestRangeWrapper
  private case class TestPrefix(r: PrefixRange[TestLeaf]) extends TestRangeWrapper
  private case class TestInequality(r: InequalitySeekRange[TestLeaf]) extends TestRangeWrapper
  private case class TestPointDistance(r: PointDistanceRange[TestLeaf]) extends TestRangeWrapper
  private case class TestPointBoundingBox(r: PointBoundingBoxRange[TestLeaf]) extends TestRangeWrapper

  private val testExtractor = new RangeExtractor[TestLeaf] {

    override def extract(wrapper: TestLeaf): ExtractedRange[TestLeaf] = wrapper match {
      case RangeLeaf(TestPrefix(r))           => ExtractedRange.Prefix(r)
      case RangeLeaf(TestInequality(r))       => ExtractedRange.Inequality(r)
      case RangeLeaf(TestPointDistance(r))    => ExtractedRange.PointDistance(r)
      case RangeLeaf(TestPointBoundingBox(r)) => ExtractedRange.PointBoundingBox(r)
      case other => throw InternalException.internalError(
          getClass.getSimpleName,
          s"Unexpected range wrapper in test: $other"
        )
    }
  }

  private def evalLeaf(leaf: TestLeaf): AnyValue = leaf match {
    case ValueLeaf(v) => v
    case RangeLeaf(_) =>
      throw new IllegalStateException("RangeLeaf shouldn't be evaluated directly as a leaf")
  }

  private def testResolver(): ValueResolver[TestLeaf] =
    new ValueResolver[TestLeaf] {
      override def eval(prepared: TestLeaf): AnyValue = evalLeaf(prepared)
    }

  // ----- Helpers -----------------------------------------------------------

  private val pid: Int = 7
  private val propIds: Array[Int] = Array(pid)

  private def runPlan(qe: QueryExpression[TestLeaf], ids: Array[Int] = propIds)
    : collection.Seq[collection.immutable.Seq[PropertyIndexQuery]] = {
    val compiled = QueryExpressionSupport.compile[TestLeaf, TestLeaf](
      qe,
      ids,
      identity[TestLeaf],
      testExtractor
    )
    compiled.apply(testResolver())
  }

  // ============================================================================
  // compile() — top-level per-case
  // ============================================================================

  test("Single-Some emits one exact predicate") {
    val qe = SingleQueryExpression(ValueLeaf(Values.intValue(5)))
    val out = runPlan(qe)
    out shouldBe Seq(Seq(PropertyIndexQuery.exact(pid, Values.intValue(5))))
  }

  test("Single with a non-storable AnyValue (MapValue) emits empty") {
    // `safeOrEmpty` returns None only for AnyValues that are neither Value nor a storable
    // ListValue (see CypherCoercions.asStorableValueOrNull). A MapValue triggers that branch.
    val qe = SingleQueryExpression(ValueLeaf(VirtualValues.EMPTY_MAP))
    val out = runPlan(qe)
    out shouldBe empty
  }

  test("Single with NO_VALUE emits exact(propId, NO_VALUE) (Value pass-through)") {
    // NO_VALUE is a Value subtype, so safeOrEmpty returns Some(NO_VALUE). The resulting
    // exact-seek finds nothing at kernel level, but the predicate itself is still emitted.
    val qe = SingleQueryExpression(ValueLeaf(Values.NO_VALUE))
    val out = runPlan(qe)
    out shouldBe Seq(Seq(PropertyIndexQuery.exact(pid, Values.NO_VALUE)))
  }

  test("Many-list emits K exact predicates (one per element)") {
    val list = VirtualValues.list(Values.intValue(1), Values.intValue(2), Values.intValue(3))
    val qe = ManyQueryExpression(ValueLeaf(list))
    val out = runPlan(qe)
    // Each element becomes its own sub-query with one exact predicate.
    out should have size 3
    out.flatten.toSet shouldBe Set(
      PropertyIndexQuery.exact(pid, Values.intValue(1)),
      PropertyIndexQuery.exact(pid, Values.intValue(2)),
      PropertyIndexQuery.exact(pid, Values.intValue(3))
    )
  }

  test("Many-list should distinct values and preserve order") {
    val list = VirtualValues.list(Values.intValue(1), Values.intValue(1), Values.intValue(2))
    val qe = ManyQueryExpression(ValueLeaf(list))
    val out = runPlan(qe)
    out should have size 2
    out(0) shouldBe Seq(PropertyIndexQuery.exact(pid, Values.intValue(1)))
    out(1) shouldBe Seq(PropertyIndexQuery.exact(pid, Values.intValue(2)))
  }

  test("Many-list with non-storable AnyValue items filters them via safeOrEmpty") {
    // Items whose safeOrEmpty returns None (e.g., MapValue — not a Value, not a storable List)
    // are dropped. NO_VALUE items are NOT filtered here (they are Values, so safeOrEmpty=Some);
    // that behavior is exercised separately via T4b.
    val list = VirtualValues.list(Values.intValue(1), VirtualValues.EMPTY_MAP, Values.intValue(2))
    val qe = ManyQueryExpression(ValueLeaf(list))
    val out = runPlan(qe)
    out should have size 2
    out.flatten.toSet shouldBe Set(
      PropertyIndexQuery.exact(pid, Values.intValue(1)),
      PropertyIndexQuery.exact(pid, Values.intValue(2))
    )
  }

  test("Many-list NO_VALUE items pass through as exact(propId, NO_VALUE)") {
    // NO_VALUE is a Value subtype so safeOrEmpty(NO_VALUE) = Some(NO_VALUE), which emits an exact predicate.
    val list = VirtualValues.list(Values.intValue(1), Values.NO_VALUE, Values.intValue(2))
    val qe = ManyQueryExpression(ValueLeaf(list))
    val out = runPlan(qe)
    out should have size 3
    out.flatten.toSet shouldBe Set(
      PropertyIndexQuery.exact(pid, Values.intValue(1)),
      PropertyIndexQuery.exact(pid, Values.NO_VALUE),
      PropertyIndexQuery.exact(pid, Values.intValue(2))
    )
  }

  test("Many-NO_VALUE emits empty") {
    val qe = ManyQueryExpression(ValueLeaf(Values.NO_VALUE))
    val out = runPlan(qe)
    out shouldBe empty
  }

  test("Many top-level with non-list Value throws expectedList") {
    val qe = ManyQueryExpression(ValueLeaf(Values.intValue(1)))
    a[CypherTypeException] should be thrownBy runPlan(qe)
  }

  test("Many top-level with non-AnyValue throws expectedList") {
    // At top level, a non-Value, non-list, non-NO_VALUE leaf falls to the `other` arm and throws expectedList.
    val qe = ManyQueryExpression(ValueLeaf(null.asInstanceOf[AnyValue]))
    // Note: pattern match order is IsList => NO_VALUE => Value => other. `null` is not
    // Values.NO_VALUE (it's a JVM null), not an IsList, and not Value (instanceof null is
    // false), so it falls to `other`.
    a[CypherTypeException] should be thrownBy runPlan(qe)
  }

  test("Range-Prefix emits one stringPrefix predicate") {
    val qe = RangeQueryExpression(RangeLeaf(TestPrefix(PrefixRange(ValueLeaf(Values.stringValue("hel"))))))
    val out = runPlan(qe)
    out shouldBe Seq(Seq(PropertyIndexQuery.stringPrefix(pid, Values.stringValue("hel"))))
  }

  test("Range-Prefix with non-TextValue emits empty") {
    // `safeOrEmpty(intValue)` returns `Some(intValue)`, but the pattern match demands a
    // TextValue subtype — falls to the `_` arm → empty.
    val qe = RangeQueryExpression(RangeLeaf(TestPrefix(PrefixRange(ValueLeaf(Values.intValue(42))))))
    val out = runPlan(qe)
    out shouldBe empty
  }

  test("Range-Inequality happy path emits one range predicate") {
    val bound = InclusiveBound(ValueLeaf(Values.intValue(5)))
    val range = RangeGreaterThan(NonEmptyList(bound))
    val qe = RangeQueryExpression(RangeLeaf(TestInequality(range)))
    val out = runPlan(qe)
    out should have size 1
    out.head should have size 1
    out.head.head shouldBe PropertyIndexQuery.range(pid, Values.intValue(5), true, null, false)
  }

  test("Range-Inequality with mismatched valueGroup emits empty") {
    val textBound = InclusiveBound(ValueLeaf(Values.stringValue("abc")))
    val numBound = InclusiveBound(ValueLeaf(Values.intValue(5)))
    val range = RangeGreaterThan(NonEmptyList(textBound, numBound))
    val qe = RangeQueryExpression(RangeLeaf(TestInequality(range)))
    val out = runPlan(qe)
    out shouldBe empty
  }

  test("Range-PointDistance Cartesian emits one bounding-box predicate") {
    val center = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 1.0)
    val pdRange = PointDistanceRange[TestLeaf](ValueLeaf(center), ValueLeaf(Values.doubleValue(2.0)), inclusive = true)
    val qe = RangeQueryExpression(RangeLeaf(TestPointDistance(pdRange)))
    val out = runPlan(qe)
    out should have size 1
    out.head should have size 1
  }

  test("Range-PointDistance WGS-84 cross-meridian emits multiple bbox sub-queries") {
    // Point on the anti-meridian with a small radius — calculator splits into 2 bboxes.
    val center = Values.pointValue(CoordinateReferenceSystem.WGS_84, -180.0, 0.0)
    val pdRange =
      PointDistanceRange[TestLeaf](ValueLeaf(center), ValueLeaf(Values.doubleValue(1000.0)), inclusive = false)
    val qe = RangeQueryExpression(RangeLeaf(TestPointDistance(pdRange)))
    val out = runPlan(qe)
    // K > 1 — exact K depends on the WGS-84 calculator's bbox split for an anti-meridian point.
    out.size should be > 1
    out.foreach(sub => sub should have size 1)
  }

  test("Range-PointDistance with non-NumberValue distance emits empty") {
    val center = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 1.0)
    val pdRange =
      PointDistanceRange[TestLeaf](ValueLeaf(center), ValueLeaf(Values.stringValue("not-a-number")), inclusive = true)
    val qe = RangeQueryExpression(RangeLeaf(TestPointDistance(pdRange)))
    val out = runPlan(qe)
    out shouldBe empty
  }

  test("Range-PointDistance with non-PointValue point emits empty") {
    val pdRange = PointDistanceRange[TestLeaf](
      ValueLeaf(Values.stringValue("not-a-point")),
      ValueLeaf(Values.doubleValue(2.0)),
      inclusive = true
    )
    val qe = RangeQueryExpression(RangeLeaf(TestPointDistance(pdRange)))
    val out = runPlan(qe)
    out shouldBe empty
  }

  test("Range-PointBoundingBox happy path emits bbox sub-queries") {
    val ll = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 0.0, 0.0)
    val ur = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 5.0, 5.0)
    val bbRange = PointBoundingBoxRange[TestLeaf](ValueLeaf(ll), ValueLeaf(ur))
    val qe = RangeQueryExpression(RangeLeaf(TestPointBoundingBox(bbRange)))
    val out = runPlan(qe)
    out should have size 1
    out.head should have size 1
  }

  test("Range-PointBoundingBox with mismatched CRS emits empty") {
    val ll = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 0.0, 0.0)
    val ur = Values.pointValue(CoordinateReferenceSystem.WGS_84, 5.0, 5.0)
    val bbRange = PointBoundingBoxRange[TestLeaf](ValueLeaf(ll), ValueLeaf(ur))
    val qe = RangeQueryExpression(RangeLeaf(TestPointBoundingBox(bbRange)))
    val out = runPlan(qe)
    out shouldBe empty
  }

  test("Range with propertyIds.length != 1 throws under -ea") {
    // `checkOnlyWhenAssertionsAreEnabled(propertyIds.length == 1)` only fires under -ea; skip otherwise.
    assume(AssertionRunner.ASSERTIONS_ENABLED, "requires -ea")
    val qe = RangeQueryExpression(RangeLeaf(TestPrefix(PrefixRange(ValueLeaf(Values.stringValue("a"))))))
    a[AssertionError] should be thrownBy QueryExpressionSupport.compile[TestLeaf, TestLeaf](
      qe,
      Array(7, 8),
      identity[TestLeaf],
      testExtractor
    )
  }

  test("Composite with mismatched member/propertyIds count throws under -ea") {
    assume(AssertionRunner.ASSERTIONS_ENABLED, "requires -ea")
    val qe = CompositeQueryExpression(Seq(
      SingleQueryExpression(ValueLeaf(Values.intValue(1))),
      SingleQueryExpression(ValueLeaf(Values.intValue(2)))
    ))
    a[AssertionError] should be thrownBy QueryExpressionSupport.compile[TestLeaf, TestLeaf](
      qe,
      Array(7),
      identity[TestLeaf],
      testExtractor
    )
  }

  // ============================================================================
  // compile() — composite-member per-case
  // ============================================================================

  test("composite Single member emits exact predicate") {
    val m1 = SingleQueryExpression(ValueLeaf(Values.intValue(1)))
    val qe = CompositeQueryExpression(Seq(m1))
    val out = runPlan(qe, Array(pid))
    out shouldBe Seq(Seq(PropertyIndexQuery.exact(pid, Values.intValue(1))))
  }

  test("composite Many-list emits distinct exact predicates per element") {
    // Composite-member Many deduplicates on the post-safeOrEmpty Value, preserving insertion order.
    val list = VirtualValues.list(Values.intValue(1), Values.intValue(1), Values.intValue(2))
    val m1 = ManyQueryExpression(ValueLeaf(list))
    val qe = CompositeQueryExpression(Seq(m1))
    val out = runPlan(qe, Array(pid))
    // Composite wraps each member's output via SeqCombiner.combine. With a single-member
    // composite and 2 distinct elements, we expect 2 sub-queries each with 1 predicate.
    out should have size 2
    out.flatten.toSet shouldBe Set(
      PropertyIndexQuery.exact(pid, Values.intValue(1)),
      PropertyIndexQuery.exact(pid, Values.intValue(2))
    )
  }

  test("composite Many-null member yields empty combined output") {
    // null leaf at composite-member Many → empty member list → combine produces empty.
    val m1 = ManyQueryExpression(ValueLeaf(null.asInstanceOf[AnyValue]))
    val m2 = SingleQueryExpression(ValueLeaf(Values.intValue(42)))
    val qe = CompositeQueryExpression(Seq(m1, m2))
    val out = runPlan(qe, Array(pid, pid + 1))
    out shouldBe empty
  }

  test("composite Many with non-list Value throws expectedCollectionWasNot") {
    val m1 = ManyQueryExpression(ValueLeaf(Values.intValue(1)))
    val m2 = SingleQueryExpression(ValueLeaf(Values.intValue(2)))
    val qe = CompositeQueryExpression(Seq(m1, m2))
    val ex = the[CypherTypeException] thrownBy runPlan(qe, Array(pid, pid + 1))
    ex.getMessage should include("to be a collection but it was not")
  }

  test("composite Range-Prefix emits stringPrefix predicate") {
    val m1 = RangeQueryExpression(RangeLeaf(TestPrefix(PrefixRange(ValueLeaf(Values.stringValue("hel"))))))
    val qe = CompositeQueryExpression(Seq(m1))
    val out = runPlan(qe, Array(pid))
    out shouldBe Seq(Seq(PropertyIndexQuery.stringPrefix(pid, Values.stringValue("hel"))))
  }

  test("composite Range-Inequality emits range predicate") {
    val range = RangeLessThan(NonEmptyList(ExclusiveBound(ValueLeaf(Values.intValue(10)))))
    val m1 = RangeQueryExpression(RangeLeaf(TestInequality(range)))
    val qe = CompositeQueryExpression(Seq(m1))
    val out = runPlan(qe, Array(pid))
    out should have size 1
    out.head should have size 1
    out.head.head shouldBe PropertyIndexQuery.range(pid, null, false, Values.intValue(10), false)
  }

  test("composite Range-PointDistance emits bbox predicates") {
    val center = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 1.0)
    val pd = PointDistanceRange[TestLeaf](ValueLeaf(center), ValueLeaf(Values.doubleValue(2.0)), inclusive = true)
    val m1 = RangeQueryExpression(RangeLeaf(TestPointDistance(pd)))
    val qe = CompositeQueryExpression(Seq(m1))
    val out = runPlan(qe, Array(pid))
    out should have size 1
    out.head should have size 1
  }

  test("composite Existence / NonExistence / All members emit exists/notExists/all") {
    val m1: QueryExpression[TestLeaf] = ExistenceQueryExpression
    val m2: QueryExpression[TestLeaf] = NonExistenceQueryExpression
    val m3: QueryExpression[TestLeaf] = AllQueryExpression
    val qe = CompositeQueryExpression(Seq(m1, m2, m3))
    val ids = Array(10, 11, 12)
    val out = runPlan(qe, ids)
    out should have size 1
    out.head shouldBe Seq(
      PropertyIndexQuery.exists(10),
      PropertyIndexQuery.notExists(11),
      PropertyIndexQuery.all(12)
    )
  }

  test("nested CompositeQueryExpression throws") {
    val inner = CompositeQueryExpression(Seq(SingleQueryExpression(ValueLeaf(Values.intValue(1)))))
    val outer = CompositeQueryExpression(Seq(inner))
    val ex = the[InternalException] thrownBy runPlan(outer, Array(pid))
    ex.getMessage should include("can't be nested in a CompositeQueryExpression")
  }

  test("composite EntityFilterQueryExpression member throws") {
    val qe = CompositeQueryExpression(Seq[QueryExpression[TestLeaf]](MatchAllQueryExpression))
    val ex = the[InternalException] thrownBy runPlan(qe, Array(pid))
    ex.getMessage should include("EntityFilterQueryExpression can only be planned for vector searches")
  }

  // ============================================================================
  // compile() — composite cross-product
  // ============================================================================

  test("2-member Single × Many composite produces M × N sub-queries") {
    val list = VirtualValues.list(Values.stringValue("a"), Values.stringValue("b"))
    val m1 = SingleQueryExpression(ValueLeaf(Values.intValue(1)))
    val m2 = ManyQueryExpression(ValueLeaf(list))
    val qe = CompositeQueryExpression(Seq(m1, m2))
    val out = runPlan(qe, Array(10, 11))
    out should have size 2 // 1 × 2
    out.foreach(sub => sub should have size 2)
  }

  test("3-member Many × Many × Single composite produces combined sub-queries") {
    val l1 = VirtualValues.list(Values.intValue(1), Values.intValue(2))
    val l2 = VirtualValues.list(Values.stringValue("a"), Values.stringValue("b"), Values.stringValue("c"))
    val m1 = ManyQueryExpression(ValueLeaf(l1))
    val m2 = ManyQueryExpression(ValueLeaf(l2))
    val m3 = SingleQueryExpression(ValueLeaf(Values.doubleValue(3.0)))
    val qe = CompositeQueryExpression(Seq(m1, m2, m3))
    val out = runPlan(qe, Array(10, 11, 12))
    out should have size 6 // 2 × 3 × 1
    out.foreach(sub => sub should have size 3)
  }

  test("composite with a zero-predicate member yields empty combined") {
    // If any member produces an empty member-predicate list, the cross-product is empty.
    // MapValue triggers safeOrEmpty=None in compositeMemberSingle → empty member list.
    val m1 = SingleQueryExpression(ValueLeaf(VirtualValues.EMPTY_MAP))
    val m2 = SingleQueryExpression(ValueLeaf(Values.intValue(42)))
    val qe = CompositeQueryExpression(Seq(m1, m2))
    val out = runPlan(qe, Array(10, 11))
    out shouldBe empty
  }

  test("composite Many uses .distinct (preserves insertion order)") {
    val l1 = VirtualValues.list(
      Values.stringValue("b"),
      Values.stringValue("a"),
      Values.stringValue("b") // duplicate — .distinct keeps the first occurrence
    )
    val m1 = ManyQueryExpression(ValueLeaf(l1))
    val qe = CompositeQueryExpression(Seq(m1))
    val out = runPlan(qe, Array(pid))
    out should have size 2
    // The first sub-query corresponds to "b", the second to "a" — composite dedup is insertion-stable.
    out(0) shouldBe Seq(PropertyIndexQuery.exact(pid, Values.stringValue("b")))
    out(1) shouldBe Seq(PropertyIndexQuery.exact(pid, Values.stringValue("a")))
  }

  // ============================================================================
  // compile() — RangeQueryExpression flatten contract
  // ============================================================================
  //
  // Asserts that `runPlan(RangeQueryExpression).flatten` matches the predicate list
  // expected by partitioned-scan callers, which consume the flattened predicate list
  // rather than the per-sub-query nesting.

  test("Range-Prefix flatten contract produces one stringPrefix predicate") {
    val qe = RangeQueryExpression(RangeLeaf(TestPrefix(PrefixRange(ValueLeaf(Values.stringValue("hel"))))))
    val flat = runPlan(qe).flatten
    flat shouldBe Seq(PropertyIndexQuery.stringPrefix(pid, Values.stringValue("hel")))
  }

  test("Range-Inequality flatten contract with valid bounds produces one range predicate") {
    val range = RangeGreaterThan(NonEmptyList(InclusiveBound(ValueLeaf(Values.intValue(5)))))
    val qe = RangeQueryExpression(RangeLeaf(TestInequality(range)))
    val flat = runPlan(qe).flatten
    flat shouldBe Seq(PropertyIndexQuery.range(pid, Values.intValue(5), true, null, false))
  }

  test("Range-PointDistance multi-bbox flatten contract produces K bbox predicates") {
    val center = Values.pointValue(CoordinateReferenceSystem.WGS_84, -180.0, 0.0)
    val pd = PointDistanceRange[TestLeaf](ValueLeaf(center), ValueLeaf(Values.doubleValue(1000.0)), inclusive = false)
    val qe = RangeQueryExpression(RangeLeaf(TestPointDistance(pd)))
    val flat = runPlan(qe).flatten
    // Multi-bbox — K depends on the WGS-84 calculator. Each predicate must be a bbox.
    flat.size should be > 1
    all(flat) shouldBe a[PropertyIndexQuery.BoundingBoxPredicate]
  }

  test("Range-PointBoundingBox flatten contract produces bbox predicates") {
    val ll = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 0.0, 0.0)
    val ur = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 5.0, 5.0)
    val bb = PointBoundingBoxRange[TestLeaf](ValueLeaf(ll), ValueLeaf(ur))
    val qe = RangeQueryExpression(RangeLeaf(TestPointBoundingBox(bb)))
    val flat = runPlan(qe).flatten
    flat should not be empty
    all(flat) shouldBe a[PropertyIndexQuery.BoundingBoxPredicate]
  }

  // ============================================================================
  // compile() — prepareLeaf invocation counting + CompiledQueryExpression reuse
  // ============================================================================

  test("compile() calls prepareLeaf exactly once per Single leaf") {
    val counter = new AtomicInteger(0)
    val prep: TestLeaf => TestLeaf = l => { counter.incrementAndGet(); l }
    val qe = SingleQueryExpression(ValueLeaf(Values.intValue(1)))
    QueryExpressionSupport.compile[TestLeaf, TestLeaf](qe, propIds, prep, testExtractor)
    counter.get() shouldBe 1
  }

  test("compile() calls prepareLeaf exactly once per Many leaf") {
    val counter = new AtomicInteger(0)
    val prep: TestLeaf => TestLeaf = l => { counter.incrementAndGet(); l }
    val qe = ManyQueryExpression(ValueLeaf(VirtualValues.list(Values.intValue(1), Values.intValue(2))))
    QueryExpressionSupport.compile[TestLeaf, TestLeaf](qe, propIds, prep, testExtractor)
    counter.get() shouldBe 1
  }

  test("compile() calls prepareLeaf exactly twice per PointDistanceRange (point + distance)") {
    val counter = new AtomicInteger(0)
    val prep: TestLeaf => TestLeaf = l => { counter.incrementAndGet(); l }
    val center = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 1.0)
    val pd = PointDistanceRange[TestLeaf](ValueLeaf(center), ValueLeaf(Values.doubleValue(2.0)), inclusive = true)
    val qe = RangeQueryExpression(RangeLeaf(TestPointDistance(pd)))
    QueryExpressionSupport.compile[TestLeaf, TestLeaf](qe, propIds, prep, testExtractor)
    counter.get() shouldBe 2
  }

  test("compile() calls prepareLeaf exactly twice per PointBoundingBoxRange") {
    val counter = new AtomicInteger(0)
    val prep: TestLeaf => TestLeaf = l => { counter.incrementAndGet(); l }
    val ll = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 0.0, 0.0)
    val ur = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 5.0, 5.0)
    val bb = PointBoundingBoxRange[TestLeaf](ValueLeaf(ll), ValueLeaf(ur))
    val qe = RangeQueryExpression(RangeLeaf(TestPointBoundingBox(bb)))
    QueryExpressionSupport.compile[TestLeaf, TestLeaf](qe, propIds, prep, testExtractor)
    counter.get() shouldBe 2
  }

  test("compile() calls prepareLeaf once per Inequality bound") {
    val counter = new AtomicInteger(0)
    val prep: TestLeaf => TestLeaf = l => { counter.incrementAndGet(); l }
    val range = RangeBetween(
      RangeGreaterThan(NonEmptyList(InclusiveBound(ValueLeaf(Values.intValue(1))))),
      RangeLessThan(NonEmptyList(ExclusiveBound(ValueLeaf(Values.intValue(10)))))
    )
    val qe = RangeQueryExpression(RangeLeaf(TestInequality(range)))
    QueryExpressionSupport.compile[TestLeaf, TestLeaf](qe, propIds, prep, testExtractor)
    counter.get() shouldBe 2
  }

  test("CompiledQueryExpression.apply() produces different outputs for different resolvers") {
    val qe = SingleQueryExpression(ValueLeaf(Values.intValue(99)))
    val compiled: CompiledQueryExpression[TestLeaf] = QueryExpressionSupport.compile[TestLeaf, TestLeaf](
      qe,
      propIds,
      identity[TestLeaf],
      testExtractor
    )
    val identityResolver = new ValueResolver[TestLeaf] {
      override def eval(prepared: TestLeaf): AnyValue = evalLeaf(prepared)
    }
    val constantResolver = new ValueResolver[TestLeaf] {
      override def eval(prepared: TestLeaf): AnyValue = Values.stringValue("overridden")
    }
    val out1 = compiled.apply(identityResolver)
    val out2 = compiled.apply(constantResolver)
    out1 shouldBe Seq(Seq(PropertyIndexQuery.exact(pid, Values.intValue(99))))
    out2 shouldBe Seq(Seq(PropertyIndexQuery.exact(pid, Values.stringValue("overridden"))))
  }

  test("ValueResolver.one is final and returns safeOrEmpty(eval(...))") {
    val resolver = new ValueResolver[TestLeaf] {
      override def eval(prepared: TestLeaf): AnyValue = Values.intValue(5)
    }
    resolver.one(ValueLeaf(Values.NO_VALUE)) shouldBe Some(Values.intValue(5))

    // Non-Value / non-storable-list AnyValue makes safeOrEmpty return None.
    val resolverNonStorable = new ValueResolver[TestLeaf] {
      override def eval(prepared: TestLeaf): AnyValue = VirtualValues.EMPTY_MAP
    }
    resolverNonStorable.one(ValueLeaf(Values.intValue(1))) shouldBe None
  }

  test("plan-time top-level ExistenceQueryExpression throws at compile()") {
    val qe: QueryExpression[TestLeaf] = ExistenceQueryExpression
    val ex = the[InternalException] thrownBy QueryExpressionSupport.compile[TestLeaf, TestLeaf](
      qe,
      propIds,
      identity[TestLeaf],
      testExtractor
    )
    ex.getMessage should include("ExistenceQueryExpression shouldn't be found outside")
  }

  test("plan-time top-level NonExistenceQueryExpression throws at compile()") {
    val qe: QueryExpression[TestLeaf] = NonExistenceQueryExpression
    val ex = the[InternalException] thrownBy QueryExpressionSupport.compile[TestLeaf, TestLeaf](
      qe,
      propIds,
      identity[TestLeaf],
      testExtractor
    )
    ex.getMessage should include("NonExistenceQueryExpression shouldn't be found outside")
  }

  test("plan-time top-level AllQueryExpression throws at compile()") {
    val qe: QueryExpression[TestLeaf] = AllQueryExpression
    val ex = the[InternalException] thrownBy QueryExpressionSupport.compile[TestLeaf, TestLeaf](
      qe,
      propIds,
      identity[TestLeaf],
      testExtractor
    )
    ex.getMessage should include("Unexpected value")
  }

  test("plan-time top-level EntityFilterQueryExpression throws at compile()") {
    val qe: QueryExpression[TestLeaf] = MatchAllQueryExpression
    val ex = the[InternalException] thrownBy QueryExpressionSupport.compile[TestLeaf, TestLeaf](
      qe,
      propIds,
      identity[TestLeaf],
      testExtractor
    )
    ex.getMessage should include("EntityFilterQueryExpression can only be planned for vector searches")
  }

  // ============================================================================
  // Direct computeIndexRangeQuery tests — locks down the public helper API
  // consumed by NodeVectorIndexSearchPipe.
  // ============================================================================

  test("None returns empty") {
    QueryExpressionSupport.computeIndexRangeQuery(None, pid) shouldBe empty
  }

  test("multi-valueGroup InequalitySeekRange returns empty") {
    // Two bounds of different value groups (TEXT + NUMBER) → no possible match → empty.
    val bound1 = InclusiveBound[Value](Values.stringValue("abc"))
    val bound2 = InclusiveBound[Value](Values.intValue(5))
    val range = RangeGreaterThan(NonEmptyList(bound1, bound2))
    QueryExpressionSupport.computeIndexRangeQuery(Some(range), pid) shouldBe empty
  }

  test("RangeGreaterThan returns a range predicate with inclusive lower bound") {
    val range = RangeGreaterThan(NonEmptyList(InclusiveBound[Value](Values.intValue(5))))
    val result = QueryExpressionSupport.computeIndexRangeQuery(Some(range), pid)
    result should have length 1
    result.head shouldBe PropertyIndexQuery.range(pid, Values.intValue(5), true, null, false)
  }

  test("RangeLessThan returns a range predicate with exclusive upper bound") {
    val range = RangeLessThan(NonEmptyList(ExclusiveBound[Value](Values.intValue(10))))
    val result = QueryExpressionSupport.computeIndexRangeQuery(Some(range), pid)
    result should have length 1
    result.head shouldBe PropertyIndexQuery.range(pid, null, false, Values.intValue(10), false)
  }

  test("RangeBetween with compare<0 returns a range predicate") {
    val range = RangeBetween(
      RangeGreaterThan(NonEmptyList(InclusiveBound[Value](Values.intValue(1)))),
      RangeLessThan(NonEmptyList(ExclusiveBound[Value](Values.intValue(10))))
    )
    val result = QueryExpressionSupport.computeIndexRangeQuery(Some(range), pid)
    result should have length 1
    result.head shouldBe PropertyIndexQuery.range(pid, Values.intValue(1), true, Values.intValue(10), false)
  }

  test("RangeBetween with compare==0 and both bounds inclusive returns an exact predicate") {
    val range = RangeBetween(
      RangeGreaterThan(NonEmptyList(InclusiveBound[Value](Values.intValue(5)))),
      RangeLessThan(NonEmptyList(InclusiveBound[Value](Values.intValue(5))))
    )
    val result = QueryExpressionSupport.computeIndexRangeQuery(Some(range), pid)
    result should have length 1
    result.head shouldBe PropertyIndexQuery.exact(pid, Values.intValue(5))
  }

  test("RangeBetween with compare==0 and one exclusive bound returns empty") {
    val range = RangeBetween(
      RangeGreaterThan(NonEmptyList(InclusiveBound[Value](Values.intValue(5)))),
      RangeLessThan(NonEmptyList(ExclusiveBound[Value](Values.intValue(5))))
    )
    val result = QueryExpressionSupport.computeIndexRangeQuery(Some(range), pid)
    result shouldBe empty
  }

  test("RangeBetween with compare>0 returns empty") {
    val range = RangeBetween(
      RangeGreaterThan(NonEmptyList(InclusiveBound[Value](Values.intValue(10)))),
      RangeLessThan(NonEmptyList(InclusiveBound[Value](Values.intValue(5))))
    )
    val result = QueryExpressionSupport.computeIndexRangeQuery(Some(range), pid)
    result shouldBe empty
  }
}
