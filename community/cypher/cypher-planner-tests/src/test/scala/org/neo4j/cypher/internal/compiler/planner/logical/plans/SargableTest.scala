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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate.PartialPredicateWrapper
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTStringNotNull
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.scalatest.OptionValues.convertOptionToValuable

class SargableTest extends CypherPlannerTestSuite with AstConstructionTestSupport {

  private val expr1 = mock[Expression]
  private val expr2 = mock[Expression]

  private val nodeA = v"a"

  test("StringRangeSeekable finds n.prop STARTS WITH 'prefix'") {
    val leftExpr = prop("a", "prop")
    val startsWith = super.startsWith(leftExpr, literalString("prefix"))
    assertMatches(startsWith) {
      case AsStringRangeSeekable(PrefixRangeSeekable(range, expr, ident, property)) =>
        range should equal(PrefixRange(literalString("prefix")))
        expr should equal(startsWith)
        ident should equal(nodeA)
        property should equal(leftExpr)
    }
  }

  test("Seekable finds Equals") {
    assertMatches(equals(expr1, expr2)) {
      case WithSeekableArgs(lhs, rhs) =>
        lhs should equal(expr1)
        rhs should equal(SingleSeekableArg(expr2))
    }
  }

  test("Seekable finds In") {
    assertMatches(in(expr1, expr2)) {
      case WithSeekableArgs(lhs, rhs) =>
        lhs should equal(expr1)
        rhs should equal(ManySeekableArgs(expr2))
    }
  }

  test("ManySeekableArgs has size hint for collections") {
    ManySeekableArgs(listOf(expr1, expr2)).sizeHint should equal(Some(2))
  }

  test("IdSeekable works") {
    val leftExpr = id(nodeA)
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    assertMatches(equals(leftExpr, expr2)) {
      case AsIdSeekable(seekable) =>
        seekable.ident should equal(nodeA)
        seekable.expr should equal(leftExpr)
        seekable.args.expr should equal(expr2)
        seekable.args.sizeHint should equal(Some(1))
    }
  }

  test("IdSeekable does not match if rhs depends on lhs variable") {
    when(expr2.dependencies).thenReturn(Set[LogicalVariable](nodeA))

    assertDoesNotMatch(equals(id(nodeA), expr2)) {
      case AsIdSeekable(_) => ( /* oh noes */ )
    }
  }

  test("IdSeekable does not match if function is not the id function") {
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    assertDoesNotMatch(equals(function("rand", nodeA), expr2)) {
      case AsIdSeekable(_) => ( /* oh noes */ )
    }
  }

  test("AsElementIdSeekable works") {
    val leftExpr = elementId(nodeA)
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    assertMatches(equals(leftExpr, expr2)) {
      case AsElementIdSeekable(seekable) =>
        seekable.ident should equal(nodeA)
        seekable.expr should equal(leftExpr)
        seekable.args.expr should equal(expr2)
        seekable.args.sizeHint should equal(Some(1))
    }
  }

  test("AsElementIdSeekable does not match if rhs depends on lhs variable") {
    when(expr2.dependencies).thenReturn(Set[LogicalVariable](nodeA))

    assertDoesNotMatch(equals(elementId(nodeA), expr2)) {
      case AsElementIdSeekable(_) => ( /* oh noes */ )
    }
  }

  test("AsElementIdSeekable does not match if function is not the id function") {
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    assertDoesNotMatch(equals(function("rand", nodeA), expr2)) {
      case AsElementIdSeekable(_) => ( /* oh noes */ )
    }
  }

  test("PropertySeekable works with plain expressions") {
    val leftExpr = prop("a", "id")
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    assertMatches(in(leftExpr, expr2)) {
      case AsPropertySeekable(seekable) =>
        seekable.ident should equal(nodeA)
        seekable.expr should equal(leftExpr)
        seekable.args.expr should equal(expr2)
        seekable.args.sizeHint should equal(None)
    }
  }

  test("PropertySeekable works with collection expressions") {
    val leftExpr = prop("a", "id")
    val rightExpr = listOf(expr1, expr2)
    when(expr1.dependencies).thenReturn(Set.empty[LogicalVariable])
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    assertMatches(in(leftExpr, rightExpr)) {
      case AsPropertySeekable(seekable) =>
        seekable.ident should equal(nodeA)
        seekable.expr should equal(leftExpr)
        seekable.args.expr should equal(rightExpr)
        seekable.args.sizeHint should equal(Some(2))
    }
  }

  test("PropertySeekable propertyValueType with ListLiteral") {
    when(expr1.dependencies).thenReturn(Set.empty[LogicalVariable])
    when(expr2.dependencies).thenReturn(Set.empty[LogicalVariable])

    val types = ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]
      .updated(expr1, ExpressionTypeInfo(TypeSpec.exact(CTFloat)))
      .updated(expr2, ExpressionTypeInfo(TypeSpec.exact(CTInteger)))

    val seekable = in(prop("a", "id"), listOf(expr1, expr2)) match {
      case AsPropertySeekable(seekable) => seekable
      case _                            => null
    }

    seekable.propertyValueType(SemanticTable(types)) should be(CTNumber)
  }

  test("PropertySeekable propertyValueType with equals") {
    when(expr1.dependencies).thenReturn(Set.empty[LogicalVariable])

    val types = ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]
      .updated(expr1, ExpressionTypeInfo(TypeSpec.exact(CTFloat)))

    val seekable = equals(prop("a", "id"), expr1) match {
      case AsPropertySeekable(seekable) => seekable
      case _                            => null
    }

    seekable.propertyValueType(SemanticTable(types)) should be(CTFloat)
  }

  test("PropertySeekable propertyValueType with Parameter") {
    val rightExpr = parameter("foo", CTString)
    val types = ASTAnnotationMap.empty[Expression, ExpressionTypeInfo]
      .updated(rightExpr, ExpressionTypeInfo(TypeSpec.exact(CTList(CTString))))

    val seekable = in(prop("a", "id"), rightExpr) match {
      case AsPropertySeekable(seekable) => seekable
      case _                            => null
    }

    seekable.propertyValueType(SemanticTable(types)) should be(CTString)
  }

  test("InequalityRangeSeekable propertyValueType") {
    val leftExpr = prop("a", "id")
    val min = literalInt(10)
    val max = literalFloat(20.5)
    val table = SemanticTable()
      .addTypeInfo(min, CTInteger.invariant)
      .addTypeInfo(max, CTFloat.invariant)
    val seekable = AndedPropertyInequalities(
      nodeA,
      leftExpr,
      NonEmptyList(greaterThan(leftExpr, min), lessThanOrEqual(leftExpr, max))
    ) match {
      case AsValueRangeSeekable(seekables) => seekables.head
      case _                               => null
    }

    seekable.propertyValueType(table) should be(CTNumber)
  }

  test("InequalityRangeSeekable propertyValueType should tolerate missing type information") {
    val leftExpr = prop("a", "id")
    val min = literalInt(10)
    val max = literalFloat(20.5)
    val table = SemanticTable()
    val seekable = AndedPropertyInequalities(
      nodeA,
      leftExpr,
      NonEmptyList(greaterThan(leftExpr, min), lessThanOrEqual(leftExpr, max))
    ) match {
      case AsValueRangeSeekable(seekables) => seekables.head
      case _                               => null
    }

    seekable.propertyValueType(table) should be(CTAny)
  }

  test("PropertySeekable does not match if rhs depends on lhs variable") {
    when(expr2.dependencies).thenReturn(Set[LogicalVariable](nodeA))

    assertDoesNotMatch(in(prop("a", "id"), expr2)) {
      case AsPropertySeekable(_) => ( /* oh noes */ )
    }
  }

  test("PropertyScannable works") {
    val propertyExpr = prop("a", "name")
    val expr = isNotNull(propertyExpr)

    assertMatches(expr) {
      case AsPropertyScannable(scannables) =>
        scannables.head.expr should equal(expr)
        scannables.head.property should equal(propertyExpr)
        scannables.head.ident should equal(nodeA)
        scannables.head.propertyKey should equal(propertyExpr.propertyKey)
    }
  }

  test("NOT a.prop = v is property scannable") {
    val expr = not(equals(prop("a", "prop"), v"v"))

    assertMatches(expr) { case AsPropertyScannable(_) => }
  }

  test("NOT a.prop IN v is not property scannable") {
    val expr = not(in(prop("a", "prop"), v"v"))

    assertDoesNotMatch(expr) { case AsPropertyScannable(_) => }
  }

  test("NOT a.prop IN empty list parameter is not property scannable") {
    val expr = not(in(prop("a", "prop"), parameter("name", CTList(CTAny), Some(0))))

    assertDoesNotMatch(expr) { case AsPropertyScannable(_) => }
  }

  test("NOT a.prop IN non-empty list parameter is property scannable") {
    val expr = not(in(prop("a", "prop"), parameter("name", CTList(CTAny), Some(1))))

    assertMatches(expr) { case AsPropertyScannable(_) => }
  }

  test("NOT a.prop IN literal empty list is not property scannable") {
    val expr = not(in(prop("a", "prop"), listOf()))

    assertDoesNotMatch(expr) { case AsPropertyScannable(_) => }
  }

  test("NOT a.prop IN literal non-empty list is not property scannable") {
    val expr = not(in(prop("a", "prop"), listOf(literal(1), literal(2))))

    assertMatches(expr) { case AsPropertyScannable(_) => }
  }

  test("NOT property IS TYPED INT NOT NULL is not property scannable") {
    val expr = not(isTyped(prop("a", "prop"), CTStringNotNull))

    assertDoesNotMatch(expr) { case AsPropertyScannable(_) => }
  }

  test("property IS TYPED INT NOT NULL is property scannable") {
    val expr = isTyped(prop("a", "prop"), CTStringNotNull)

    assertMatches(expr) { case AsPropertyScannable(_) => }
  }

  def pointDistanceFunction(arg1: Expression, arg2: Expression): FunctionInvocation = {
    FunctionInvocation(
      FunctionName(
        Namespace(List("point"))(pos),
        "distance"
      )(pos),
      distinct = false,
      IndexedSeq(
        arg1,
        arg2
      )
    )(pos)
  }

  test(
    "Three scannable properties for a point.distance comparison"
  ) {
    val aProp1 = prop(v"a", "prop1")
    val bProp2 = prop(v"b", "prop2")
    val cProp3 = prop(v"c", "prop3")

    // point.distance(a.prop1, b.prop2)
    val pointDistanceAProp1BProp2 = pointDistanceFunction(aProp1, bProp2)

    // ... > point.distance(a.prop1, b.prop2)
    val gt = greaterThan(cProp3, pointDistanceAProp1BProp2)
    // ... >= point.distance(a.prop1, b.prop2)
    val gte = greaterThanOrEqual(cProp3, pointDistanceAProp1BProp2)

    for (ineq <- Seq(gt, gte)) {
      // unapply of AsDistanceSeekable
      val distanceSeekablesFromIneq = AsDistanceSeekable.unapply(ineq).value
      distanceSeekablesFromIneq.iterator.toList should contain theSameElementsInOrderAs List(
        PointDistanceSeekable(
          v"a",
          aProp1,
          PointDistanceRange(bProp2, cProp3, inclusive = ineq.includeEquality),
          ineq
        ),
        PointDistanceSeekable(
          v"b",
          bProp2,
          PointDistanceRange(aProp1, cProp3, inclusive = ineq.includeEquality),
          ineq
        )
      )

      // unapply of AsPropertyScannable
      val propertyScannablesFromIneq = AsPropertyScannable.unapply(ineq).value
      propertyScannablesFromIneq.iterator.toList should contain theSameElementsInOrderAs List(
        ImplicitlyPropertyScannable(
          PartialPredicateWrapper(isNotNull(aProp1), ineq),
          v"a",
          aProp1,
          solvesPredicate = false,
          CTPoint,
          safelyScannableWhenNegated = true
        ),
        ImplicitlyPropertyScannable(
          PartialPredicateWrapper(isNotNull(bProp2), ineq),
          v"b",
          bProp2,
          solvesPredicate = false,
          CTPoint,
          safelyScannableWhenNegated = true
        ),
        ImplicitlyPropertyScannable(
          PartialPredicateWrapper(isNotNull(cProp3), ineq),
          v"c",
          cProp3,
          solvesPredicate = false,
          CTAny,
          safelyScannableWhenNegated = true
        )
      )
    }
  }

  test(
    "Two scannable properties for a point.distance function with two properties that is less than (or equal to) some value"
  ) {
    val aProp1 = prop(v"a", "prop1")
    val bProp2 = prop(v"b", "prop2")

    // point.distance(a.prop1, b.prop2)
    val pointDistanceAProp1BProp2 = pointDistanceFunction(aProp1, bProp2)

    // point.distance(a.prop1, b.prop2) < ...
    val lt = lessThan(pointDistanceAProp1BProp2, literalInt(5))
    // point.distance(a.prop1, b.prop2) <= ...
    val lte = lessThanOrEqual(pointDistanceAProp1BProp2, literalInt(5))
    // ... > point.distance(a.prop1, b.prop2)
    val gt = greaterThan(literalInt(5), pointDistanceAProp1BProp2)
    // ... >= point.distance(a.prop1, b.prop2)
    val gte = greaterThanOrEqual(literalInt(5), pointDistanceAProp1BProp2)

    for (ineq <- Seq(lt, lte, gt, gte)) {
      // unapply of AsDistanceSeekable
      val distanceSeekablesFromIneq = AsDistanceSeekable.unapply(ineq).value
      distanceSeekablesFromIneq.iterator.toList should contain theSameElementsInOrderAs List(
        PointDistanceSeekable(
          v"a",
          aProp1,
          PointDistanceRange(bProp2, literalInt(5), inclusive = ineq.includeEquality),
          ineq
        ),
        PointDistanceSeekable(
          v"b",
          bProp2,
          PointDistanceRange(aProp1, literalInt(5), inclusive = ineq.includeEquality),
          ineq
        )
      )

      // unapply of AsPropertyScannable
      val propertyScannablesFromIneq = AsPropertyScannable.unapply(ineq).value
      propertyScannablesFromIneq.iterator.toList should contain theSameElementsInOrderAs List(
        ImplicitlyPropertyScannable(
          PartialPredicateWrapper(isNotNull(aProp1), ineq),
          v"a",
          aProp1,
          solvesPredicate = false,
          CTPoint,
          safelyScannableWhenNegated = true
        ),
        ImplicitlyPropertyScannable(
          PartialPredicateWrapper(isNotNull(bProp2), ineq),
          v"b",
          bProp2,
          solvesPredicate = false,
          CTPoint,
          safelyScannableWhenNegated = true
        )
      )
    }
  }

  test(
    "One scannable properties for a point.distance function with one properties that is less than (or equal to) some value"
  ) {
    val aProp1 = prop(v"a", "prop1")
    val pt = point(10, 20)

    // point.distance(a.prop1, point({x: 10, y: 20}))
    val pointDistance1 = pointDistanceFunction(aProp1, pt)
    // point.distance(point({x: 10, y: 20}), a.prop1)
    val pointDistance2 = pointDistanceFunction(pt, aProp1)

    for (pointDistance <- Seq(pointDistance1, pointDistance2)) {

      // point.distance(..., ...) < ...
      val lt1 = lessThan(pointDistance, literalInt(5))
      // point.distance(..., ...) <= ...
      val lte1 = lessThanOrEqual(pointDistance, literalInt(5))
      // ... > point.distance(..., ...)
      val gt1 = greaterThan(literalInt(5), pointDistance)
      // ... >= point.distance(..., ...)
      val gte1 = greaterThanOrEqual(literalInt(5), pointDistance)

      for (ineq <- Seq(lt1, lte1, gt1, gte1)) {
        // unapply of AsDistanceSeekable
        val distanceSeekablesFromIneq = AsDistanceSeekable.unapply(ineq).value
        distanceSeekablesFromIneq.iterator.toList should contain theSameElementsInOrderAs List(
          PointDistanceSeekable(
            v"a",
            aProp1,
            PointDistanceRange(pt, literalInt(5), inclusive = ineq.includeEquality),
            ineq
          )
        )

        // unapply of AsPropertyScannable
        val propertyScannablesFromIneq = AsPropertyScannable.unapply(ineq).value
        propertyScannablesFromIneq.iterator.toList should contain theSameElementsInOrderAs List(
          ImplicitlyPropertyScannable(
            PartialPredicateWrapper(isNotNull(aProp1), ineq),
            v"a",
            aProp1,
            solvesPredicate = false,
            CTPoint,
            safelyScannableWhenNegated = true
          )
        )
      }
    }
  }

  test(
    "No scannable properties for a point.distance function with none properties that is less than (or equal to) some value"
  ) {
    val pt1 = point(10, 20)
    val pt2 = point(20, 25)

    // point.distance(a.prop1, b.prop2)
    val pointDistance1 = pointDistanceFunction(pt1, pt2)

    // point.distance(..., ...) < ...
    val lt = lessThan(pointDistance1, literalInt(5))
    // point.distance(..., ...) <= ...
    val lte = lessThanOrEqual(pointDistance1, literalInt(5))
    // ... > point.distance(..., ...)
    val gt = greaterThan(literalInt(5), pointDistance1)
    // ... >= point.distance(..., ...)
    val gte = greaterThanOrEqual(literalInt(5), pointDistance1)

    for (ineq <- Seq(lt, lte, gt, gte)) {
      // unapply of AsDistanceSeekable
      val unapplyAsDistanceSeekable = AsDistanceSeekable.unapply(ineq)
      unapplyAsDistanceSeekable.isEmpty shouldBe true

      // unapply of AsPropertyScannable
      val unapplyAsPropertyScannable = AsPropertyScannable.unapply(ineq)
      unapplyAsPropertyScannable.isEmpty shouldBe true
    }
  }

  test("Negation of pointDistance predicate can use index scan") {
    val aProp1 = prop(v"a", "prop1")
    val bProp2 = prop(v"b", "prop2")
    val notDistanceLessThan5 = not(greaterThan(pointDistanceFunction(aProp1, bProp2), literalInt(5)))
    val notDistanceLessThanOrEqualTo5 = not(lessThanOrEqual(pointDistanceFunction(aProp1, bProp2), literalInt(5)))
    val notDistanceGreaterThan5 = not(greaterThan(pointDistanceFunction(aProp1, bProp2), literalInt(5)))
    val notDistanceGreaterThanOrEqualTo5 = not(greaterThanOrEqual(pointDistanceFunction(aProp1, bProp2), literalInt(5)))
    for (
      notIneq <- Seq(
        notDistanceLessThan5,
        notDistanceLessThanOrEqualTo5,
        notDistanceGreaterThan5,
        notDistanceGreaterThanOrEqualTo5
      )
    ) {
      val asPropertyScannables = AsPropertyScannable.unapply(notIneq).value
      asPropertyScannables.iterator.toList should contain theSameElementsInOrderAs List(
        ImplicitlyPropertyScannable(
          PartialPredicateWrapper(isNotNull(aProp1), notIneq),
          v"a",
          aProp1,
          solvesPredicate = false,
          CTPoint,
          safelyScannableWhenNegated = true
        ),
        ImplicitlyPropertyScannable(
          PartialPredicateWrapper(isNotNull(bProp2), notIneq),
          v"b",
          bProp2,
          solvesPredicate = false,
          CTPoint,
          safelyScannableWhenNegated = true
        )
      )
    }
  }

  test("should produce scannable property for both sides of equality predicate") {
    val axProp = prop("a", "x")
    val byProp = prop("b", "y")
    val equalsPred = equals(axProp, byProp)

    assertMatches(equalsPred) {
      case AsPropertyScannable(scannables) =>
        scannables.iterator.toList shouldEqual List(
          ImplicitlyPropertyScannable(
            PartialPredicateWrapper(isNotNull(axProp), equalsPred),
            v"a",
            axProp,
            solvesPredicate = false,
            CTAny,
            safelyScannableWhenNegated = true
          ),
          ImplicitlyPropertyScannable(
            PartialPredicateWrapper(isNotNull(byProp), equalsPred),
            v"b",
            byProp,
            solvesPredicate = false,
            CTAny,
            safelyScannableWhenNegated = true
          )
        )
    }
  }

  test("should produce scannable property for both sides of inequality predicates") {
    val axProp = prop("a", "x")
    val byProp = prop("b", "y")
    val predicateFactories: Seq[(Expression, Expression) => Expression] =
      Seq(lessThan, lessThanOrEqual, greaterThan, greaterThanOrEqual)

    for (predicateFactory <- predicateFactories) {
      val predicate = predicateFactory(axProp, byProp)
      assertMatches(predicate) {
        case AsPropertyScannable(scannables) =>
          scannables.iterator.toList shouldEqual List(
            ImplicitlyPropertyScannable(
              PartialPredicateWrapper(isNotNull(axProp), predicate),
              v"a",
              axProp,
              solvesPredicate = false,
              CTAny,
              safelyScannableWhenNegated = true
            ),
            ImplicitlyPropertyScannable(
              PartialPredicateWrapper(isNotNull(byProp), predicate),
              v"b",
              byProp,
              solvesPredicate = false,
              CTAny,
              safelyScannableWhenNegated = true
            )
          )
      }
    }
  }

  test("should produce scannable property for both sides of string predicates") {
    val axProp = prop("a", "x")
    val byProp = prop("b", "y")
    val predicateFactories: Seq[(Expression, Expression) => Expression] =
      Seq(startsWith, contains, endsWith, AstConstructionTestSupport.regex)

    for (predicateFactory <- predicateFactories) {
      val predicate = predicateFactory(axProp, byProp)
      assertMatches(predicate) {
        case AsPropertyScannable(scannables) =>
          scannables.iterator.toList shouldEqual List(
            ImplicitlyPropertyScannable(
              PartialPredicateWrapper(isNotNull(axProp), predicate),
              v"a",
              axProp,
              solvesPredicate = false,
              CTString,
              safelyScannableWhenNegated = true
            ),
            ImplicitlyPropertyScannable(
              PartialPredicateWrapper(isNotNull(byProp), predicate),
              v"b",
              byProp,
              solvesPredicate = false,
              CTString,
              safelyScannableWhenNegated = true
            )
          )
      }
    }
  }

  private def assertMatches[T](item: Expression)(pf: PartialFunction[Expression, T]) =
    if (pf.isDefinedAt(item)) pf(item) else fail(s"Failed to match: $item")

  private def assertDoesNotMatch[T](item: Expression)(pf: PartialFunction[Expression, T]): Unit =
    if (pf.isDefinedAt(item)) fail(s"Erroneously matched: $item")
}
