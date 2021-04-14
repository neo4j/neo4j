package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexContainsScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexEndsWithScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexSeekPlanProvider
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NodeIndexLeafPlannerTest  extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private def nodeIndexLeafPlanner(restrictions: LeafPlanRestrictions) =
    NodeIndexPlanner(Seq(nodeIndexSeekPlanProvider, nodeIndexContainsScanPlanProvider, nodeIndexEndsWithScanPlanProvider, nodeIndexScanPlanProvider), restrictions)

  test("plans all possible index plans") {

    val lit42: Expression = literalInt(42)
    val lit6: Expression = literalInt(6)

    val nProp = prop("n", "prop")
    val mProp = prop("m", "prop")
    val oProp = prop("o", "prop")
    val xProp = prop("x", "prop")
    val nPropEqualsXProp = Equals(nProp, xProp)(pos)
    val nPropEqualsLit42 = equals(lit42, nProp)
    val nPropInLit6Lit42 = in(nProp, listOf(lit6, lit42))
    val nPropLessThanLit6 = lessThan(nProp, lit6)
    val litFoo = literalString("foo")
    val nPropStartsWithLitFoo = startsWith(nProp, litFoo)
    val nPropEndsWithLitFoo = endsWith(nProp, litFoo)
    val nPropContainsLitFoo = contains(nProp, litFoo)
    val point = function("point", mapOfInt("x" -> 1, "y" -> 2))
    val nPropDistance = greaterThan(lit42, function("distance", nProp, point))
    val mPropEqualsXProp = Equals(mProp, xProp)(pos)
    val mPropIsNotNull = isNotNull(mProp)
    val oPropIsNotNull = isNotNull(oProp)
    val oPropExists = exists(oProp)
    new given {
      addTypeToSemanticTable(nProp, CTInteger.invariant)
      addTypeToSemanticTable(mProp, CTInteger.invariant)
      addTypeToSemanticTable(oProp, CTInteger.invariant)
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit6, CTInteger.invariant)

      val predicates: Set[Expression] = Set(
        hasLabels("n", "Awesome"),
        hasLabels("m", "Awesome"),
        hasLabels("o", "Awesome"),
        hasLabels("x", "Awesome"),
        nPropInLit6Lit42,
        nPropLessThanLit6,
        nPropEqualsLit42,
        nPropStartsWithLitFoo,
        nPropEndsWithLitFoo,
        nPropContainsLitFoo,
        nPropDistance,
        nPropEqualsXProp,
        mPropEqualsXProp,
        mPropIsNotNull,
        oPropIsNotNull,
        oPropExists,
      )

      qg = QueryGraph(
        selections = Selections(predicates.flatMap(_.asPredicates)),
        patternNodes = Set("n", "m", "o", "x"),
        argumentIds = Set("x")
      )

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val restriction = LeafPlanRestrictions.OnlyIndexSeekPlansFor("m", Set("x"))
      val resultPlans = nodeIndexLeafPlanner(restriction)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val labelToken = LabelToken("Awesome", LabelId(0))
      val propToken = PropertyKeyToken("prop", PropertyKeyId(0))

      val plansByType = resultPlans.groupBy(_.productPrefix).mapValues(_.toSet)

      plansByType.keySet shouldEqual Set(
        "NodeIndexSeek",
        "NodeIndexContainsScan",
        "NodeIndexEndsWithScan",
        "NodeIndexScan",
      )

      plansByType("NodeIndexSeek") shouldEqual Set(
        // nPropInLit6Lit42
        NodeIndexSeek("n", labelToken, Seq(IndexedProperty(propToken, CanGetValue)), ManyQueryExpression(listOf(lit6, lit42)), Set("x"), IndexOrderNone),
        // nPropLessThanLit6
        NodeIndexSeek("n", labelToken, Seq(IndexedProperty(propToken, DoNotGetValue)), RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(lit6))))(pos)), Set("x"), IndexOrderNone),
        // nPropEqualsLit42
        NodeIndexSeek("n", labelToken, Seq(IndexedProperty(propToken, CanGetValue)), SingleQueryExpression(lit42), Set("x"), IndexOrderNone),
        // nPropStartsWithLitFoo
        NodeIndexSeek("n", labelToken, Seq(IndexedProperty(propToken, DoNotGetValue)), RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(litFoo))(pos)), Set("x"), IndexOrderNone),
        // nPropDistance
        NodeIndexSeek("n", labelToken, Seq(IndexedProperty(propToken, DoNotGetValue)), RangeQueryExpression(PointDistanceSeekRangeWrapper(PointDistanceRange(point, lit42, inclusive = false))(pos)), Set("x"), IndexOrderNone),
        // nPropEqualsXProp
        NodeIndexSeek("n", labelToken, Seq(IndexedProperty(propToken, CanGetValue)), SingleQueryExpression(xProp), Set("x"), IndexOrderNone),
        // mPropEqualsXProp
        NodeIndexSeek("m", labelToken, Seq(IndexedProperty(propToken, CanGetValue)), SingleQueryExpression(xProp), Set("x"), IndexOrderNone),
      )

      plansByType("NodeIndexContainsScan") shouldEqual Set(
        // nPropContainsLitFoo
        NodeIndexContainsScan("n", labelToken, IndexedProperty(propToken, DoNotGetValue), litFoo, Set("x"), IndexOrderNone),
      )

      plansByType("NodeIndexEndsWithScan") shouldEqual Set(
        // nPropEndsWithLitFoo
        NodeIndexEndsWithScan("n", labelToken, IndexedProperty(propToken, DoNotGetValue), litFoo, Set("x"), IndexOrderNone),
      )

      plansByType("NodeIndexScan") shouldEqual Set(
        // ..several..
        NodeIndexScan("n", labelToken, Seq(IndexedProperty(propToken, DoNotGetValue)), Set("x"), IndexOrderNone),
        // oPropIsNotNull, oPropExists
        NodeIndexScan("o", labelToken, Seq(IndexedProperty(propToken, DoNotGetValue)), Set("x"), IndexOrderNone),
      )

    }
  }


}
