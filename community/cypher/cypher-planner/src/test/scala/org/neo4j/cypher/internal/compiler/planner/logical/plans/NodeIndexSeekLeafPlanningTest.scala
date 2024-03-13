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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexSeekPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.mergeNodeUniqueIndexSeekLeafPlanner
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.PrefixRange
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.SetExtractor
import org.neo4j.graphdb.schema.IndexType

import scala.language.reflectiveCalls

class NodeIndexSeekLeafPlanningTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val n = v"n"
  private val nProp = prop("n", "prop")
  private val nFoo = prop("n", "foo")
  private val lit42: Expression = literalInt(42)
  private val lit6: Expression = literalInt(6)

  private val nPropInLit42 = in(nProp, listOf(lit42))
  private val nPropEndsWithLitText = endsWith(nProp, literalString("Text"))
  private val nPropContainsLitText = contains(nProp, literalString("Text"))
  private val nFooEqualsLit42 = equals(nFoo, lit42)
  private val nFooIsNotNull = isNotNull(nFoo)
  private val nPropLessThanLit42 = AndedPropertyInequalities(v"n", nProp, NonEmptyList(lessThan(nProp, lit42)))

  private def hasLabel(l: String) = hasLabels("n", l)

  private def indexSeekLeafPlanner(restrictions: LeafPlanRestrictions) =
    NodeIndexLeafPlanner(Seq(nodeIndexSeekPlanProvider), restrictions)

  test("does not plan index seek when no index exist") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, hasLabel("Awesome"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does plan unique index seek when the index is unique") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, hasLabel("Awesome"))
      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans.head should beLike {
        case _: NodeUniqueIndexSeek => ()
      }
    }
  }

  test("index seek with values (equality predicate) when there is an index on the property") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, hasLabel("Awesome"))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexSeek(
            `n`,
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            SingleQueryExpression(`lit42`),
            _,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("index seeks when there is an index on the property and there are multiple predicates") {
    val prop1: Property = prop("n", "prop")
    val prop1Predicate1 = in(prop1, listOf(lit42))
    val prop1Predicate2 = AndedPropertyInequalities(v"n", prop1, NonEmptyList(lessThan(prop1, lit6)))
    val prop1Predicate1Expr = SingleQueryExpression(lit42)
    val prop1Predicate2Expr =
      RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(lit6))))(pos))

    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit6, CTInteger.invariant)
      qg = queryGraph(prop1Predicate1, prop1Predicate2, hasLabel("Awesome"))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val labelToken = LabelToken("Awesome", LabelId(0))
      val prop1Token = PropertyKeyToken("prop", PropertyKeyId(0))

      val expected = Set(
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(prop1Token, CanGetValue, NODE_TYPE)),
          prop1Predicate1Expr,
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(prop1Token, DoNotGetValue, NODE_TYPE)),
          prop1Predicate2Expr,
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        )
      )

      resultPlans shouldEqual expected
    }
  }

  test("index seeks when there is a composite index and there are multiple predicates") {

    val lit43: Expression = literalInt(43)
    val lit44: Expression = literalInt(44)
    val prop1: Property = prop("n", "prop")
    val prop2: Property = prop("n", "prop2")
    val prop1Predicate1 = in(prop1, listOf(lit42))
    val prop1Predicate2 = in(prop1, listOf(lit43))
    val prop1Predicate3 = in(prop1, listOf(lit44))
    val prop2Predicate1 = in(prop2, listOf(lit6))
    val prop2Predicate2 = AndedPropertyInequalities(v"n", prop2, NonEmptyList(lessThan(prop2, lit6)))
    val prop1Predicate1Expr = SingleQueryExpression(lit42)
    val prop1Predicate2Expr = SingleQueryExpression(lit43)
    val prop1Predicate3Expr = SingleQueryExpression(lit44)
    val prop2Predicate1Expr = SingleQueryExpression(lit6)
    val prop2Predicate2Expr =
      RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(lit6))))(pos))

    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit6, CTInteger.invariant)

      qg = queryGraph(
        prop1Predicate1,
        prop1Predicate2,
        prop2Predicate1,
        prop2Predicate2,
        prop1Predicate3,
        hasLabel("Awesome")
      )

      indexOn("Awesome", "prop", "prop2")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val labelToken = LabelToken("Awesome", LabelId(0))
      val prop1Token = PropertyKeyToken("prop", PropertyKeyId(0))
      val prop2Token = PropertyKeyToken("prop2", PropertyKeyId(1))

      val expected = Set(
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(prop1Token, CanGetValue, NODE_TYPE), IndexedProperty(prop2Token, CanGetValue, NODE_TYPE)),
          CompositeQueryExpression(Seq(prop1Predicate1Expr, prop2Predicate1Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(
            IndexedProperty(prop1Token, CanGetValue, NODE_TYPE),
            IndexedProperty(prop2Token, DoNotGetValue, NODE_TYPE)
          ),
          CompositeQueryExpression(Seq(prop1Predicate1Expr, prop2Predicate2Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(prop1Token, CanGetValue, NODE_TYPE), IndexedProperty(prop2Token, CanGetValue, NODE_TYPE)),
          CompositeQueryExpression(Seq(prop1Predicate2Expr, prop2Predicate1Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(
            IndexedProperty(prop1Token, CanGetValue, NODE_TYPE),
            IndexedProperty(prop2Token, DoNotGetValue, NODE_TYPE)
          ),
          CompositeQueryExpression(Seq(prop1Predicate2Expr, prop2Predicate2Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(prop1Token, CanGetValue, NODE_TYPE), IndexedProperty(prop2Token, CanGetValue, NODE_TYPE)),
          CompositeQueryExpression(Seq(prop1Predicate3Expr, prop2Predicate1Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(
            IndexedProperty(prop1Token, CanGetValue, NODE_TYPE),
            IndexedProperty(prop2Token, DoNotGetValue, NODE_TYPE)
          ),
          CompositeQueryExpression(Seq(prop1Predicate3Expr, prop2Predicate2Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        )
      )

      resultPlans shouldEqual expected
    }
  }

  test("index seeks when there are multiple composite indexes and there are multiple predicates") {

    val lit43: Expression = literalInt(43)
    val prop1: Property = prop("n", "prop")
    val prop2: Property = prop("n", "prop2")
    val prop1Predicate1 = in(prop1, listOf(lit42))
    val prop1Predicate2 = in(prop1, listOf(lit43))
    val prop2Predicate1 = in(prop2, listOf(lit6))
    val prop1Predicate1Expr = SingleQueryExpression(lit42)
    val prop1Predicate2Expr = SingleQueryExpression(lit43)
    val prop2Predicate1Expr = SingleQueryExpression(lit6)

    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit6, CTInteger.invariant)

      qg = queryGraph(prop2Predicate1, prop1Predicate1, prop1Predicate2, hasLabel("Awesome"))

      indexOn("Awesome", "prop", "prop2")
      indexOn("Awesome", "prop2", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val labelToken = LabelToken("Awesome", LabelId(0))
      val prop1Token = PropertyKeyToken("prop", PropertyKeyId(0))
      val prop2Token = PropertyKeyToken("prop2", PropertyKeyId(1))

      val expected = Set(
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(prop1Token, CanGetValue, NODE_TYPE), IndexedProperty(prop2Token, CanGetValue, NODE_TYPE)),
          CompositeQueryExpression(Seq(prop1Predicate1Expr, prop2Predicate1Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(prop1Token, CanGetValue, NODE_TYPE), IndexedProperty(prop2Token, CanGetValue, NODE_TYPE)),
          CompositeQueryExpression(Seq(prop1Predicate2Expr, prop2Predicate1Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(prop2Token, CanGetValue, NODE_TYPE), IndexedProperty(prop1Token, CanGetValue, NODE_TYPE)),
          CompositeQueryExpression(Seq(prop2Predicate1Expr, prop1Predicate1Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(prop2Token, CanGetValue, NODE_TYPE), IndexedProperty(prop1Token, CanGetValue, NODE_TYPE)),
          CompositeQueryExpression(Seq(prop2Predicate1Expr, prop1Predicate2Expr)),
          Set(),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        )
      )

      resultPlans shouldEqual expected
    }
  }

  test(
    "index seeks when there is a composite index and there are multiple predicates that do not cover all properties"
  ) {

    val lit43: Expression = literalInt(43)
    val prop1: Property = prop("n", "prop")
    val prop2: Property = prop("n", "prop2")
    val prop1Predicate1 = in(prop1, listOf(lit42))
    val prop1Predicate2 = in(prop1, listOf(lit43))
    val prop2Predicate1 = in(prop2, listOf(lit6))
    val prop2Predicate2 = AndedPropertyInequalities(v"n", prop2, NonEmptyList(lessThan(prop2, lit6)))

    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit6, CTInteger.invariant)

      qg = queryGraph(prop1Predicate1, prop1Predicate2, prop2Predicate1, prop2Predicate2, hasLabel("Awesome"))

      indexOn("Awesome", "prop", "prop2", "prop3")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val expected = Set()

      resultPlans shouldEqual expected
    }
  }

  test("index seek without values when there is an index on the property") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropLessThanLit42, hasLabel("Awesome"))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexSeek(
            `n`,
            _,
            Seq(IndexedProperty(_, DoNotGetValue, NODE_TYPE)),
            _,
            _,
            _,
            _,
            _
          )) =>
          ()
      }
    }
  }

  test("index seek with values (from index) when there is an index on the property") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropLessThanLit42, hasLabel("Awesome"))

      indexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexSeek(
            `n`,
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            _,
            _,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("index seek with values (equality predicate) when there is a composite index on two properties") {
    new givenConfig {
      addTypeToSemanticTable(lit6, CTInteger.invariant)
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      private val inPredicate2 = in(prop("n", "prop2"), listOf(lit6))
      qg = queryGraph(nPropInLit42, inPredicate2, hasLabel("Awesome"))

      indexOn("Awesome", "prop", "prop2")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      val expectedPlans = Set(
        new LogicalPlanBuilder(wholePlan = false).nodeIndexOperator(
          "n:Awesome(prop = 42, prop2 = 6)",
          getValue = Map("prop" -> CanGetValue, "prop2" -> CanGetValue),
          supportPartitionedScan = false
        ).build()
      )

      // then
      resultPlans shouldEqual expectedPlans
    }
  }

  test(
    "index seek with values (equality predicate) when there is a composite index on two properties in the presence of other nodes, labels and properties"
  ) {
    new givenConfig {
      addTypeToSemanticTable(lit6, CTInteger.invariant)
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      private val litFoo = literalString("foo")
      addTypeToSemanticTable(litFoo, CTString.invariant)

      // MATCH (n:Awesome:Sauce), (m:Awesome)
      // WHERE n.prop = 42 AND n.prop2 = 6 AND n.prop3 = "foo" AND m.prop = "foo"
      qg = queryGraph(
        // node 'n'
        hasLabel("Awesome"),
        hasLabel("Sauce"),
        in(prop("n", "prop"), listOf(lit42)),
        in(prop("n", "prop2"), listOf(lit6)),
        in(prop("n", "prop3"), listOf(litFoo)),
        // node 'm'
        hasLabels("m", "Awesome"),
        in(prop("m", "prop"), listOf(litFoo))
      )

      // CREATE INDEX FOR (n:Awesome) ON (n.prop, n.prop2)
      indexOn("Awesome", "prop", "prop2")

    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)
      val expectedPlans = Set(
        new LogicalPlanBuilder(wholePlan = false).nodeIndexOperator(
          "n:Awesome(prop = 42, prop2 = 6)",
          getValue = Map("prop" -> CanGetValue, "prop2" -> CanGetValue),
          supportPartitionedScan = false
        ).build()
      )
      // then
      resultPlans shouldEqual expectedPlans
    }
  }

  test("index seek with values (equality predicate) when there is a composite index on many properties") {
    val propertyNames = (0 to 10).map(n => s"prop$n")
    val properties = propertyNames.map(n => prop("n", n))
    val values = (0 to 10).map(n => literalInt(n * 10 + 2))
    val predicates = properties.zip(values).map { pair =>
      val predicate = in(pair._1, listOf(pair._2))
      Predicate(Set(n), predicate)
    }

    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      values.foreach(addTypeToSemanticTable(_, CTInteger.invariant))
      qg = QueryGraph(
        selections = Selections(predicates.toSet + Predicate(Set(n), hasLabel("Awesome"))),
        patternNodes = Set(n)
      )

      indexOn("Awesome", propertyNames: _*)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexSeek(
            `n`,
            LabelToken("Awesome", _),
            props @ Seq(_*),
            CompositeQueryExpression(vals @ Seq(_*)),
            _,
            _,
            _,
            _
          )) if assertPropsAndValuesMatch(propertyNames, values, props, vals.flatMap(_.expressions)) => ()
      }
    }
  }

  private def assertPropsAndValuesMatch(
    expectedProps: Seq[String],
    expectedVals: Seq[Expression],
    foundProps: Seq[IndexedProperty],
    foundVals: Seq[Expression]
  ) = {
    val expected = expectedProps.zip(expectedVals).toMap
    val found = foundProps.map(_.propertyKeyToken.name).zip(foundVals).toMap
    found.equals(expected)
  }

  test("plans index seeks when variable exists as an argument") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      // GIVEN 42 as x MATCH a WHERE a.prop IN [x]
      val x: Expression = v"x"
      qg = queryGraph(in(nProp, listOf(x)), hasLabel("Awesome")).addArgumentIds(Seq(v"x"))

      addTypeToSemanticTable(x, CTNode.invariant)
      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val x = cfg.x
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexSeek(`n`, _, _, SingleQueryExpression(`x`), _, _, _, _)) => ()
      }
    }
  }

  test("does not plan an index seek when the RHS expression does not have its dependencies in scope") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      // MATCH a, x WHERE a.prop IN [x]
      qg = queryGraph(in(nProp, listOf(v"x")), hasLabel("Awesome"))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("plans only index plans that match the dependencies of the restriction") {
    val nProp = prop("n", "prop")
    val xProp = prop("x", "prop")
    val nPropEqualsXProp = Equals(nProp, xProp)(pos)
    val xPropExpr = SingleQueryExpression(xProp)
    val nPropEquals = equals(lit42, nProp)
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit6, CTInteger.invariant)
      addTypeToSemanticTable(nProp, CTInteger.invariant)
      val predicates: Set[Expression] = Set(
        hasLabels("n", "Awesome"),
        hasLabels("x", "Awesome"),
        in(nProp, listOf(lit42)),
        AndedPropertyInequalities(v"n", nProp, NonEmptyList(lessThan(nProp, lit6))),
        nPropEquals,
        startsWith(nProp, literalString("foo")),
        endsWith(nProp, literalString("foo")),
        contains(nProp, literalString("foo")),
        greaterThan(lit42, function(List("point"), "distance", nProp, function("point", mapOfInt("x" -> 1, "y" -> 2)))),
        nPropEqualsXProp
      )

      qg = QueryGraph(
        selections = Selections(predicates.flatMap(_.asPredicates)),
        patternNodes = Set(v"n", v"x"),
        argumentIds = Set(v"x")
      )

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val restriction = LeafPlanRestrictions.OnlyIndexSeekPlansFor(v"n", Set(v"x"))
      val resultPlans = indexSeekLeafPlanner(restriction)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val labelToken = LabelToken("Awesome", LabelId(0))
      val nPropToken = PropertyKeyToken("prop", PropertyKeyId(0))

      val expected = Set(
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(nPropToken, CanGetValue, NODE_TYPE)),
          xPropExpr,
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        )
      )

      resultPlans shouldEqual expected
    }
  }

  test("plans only index plans that match the dependencies of the restriction for composite index") {
    val xProp = prop("x", "prop")
    val yProp = prop("y", "prop")
    val xPropExpr: QueryExpression[Expression] = SingleQueryExpression(xProp)
    val yPropExpr: QueryExpression[Expression] = SingleQueryExpression(yProp)
    val lit42Expr: QueryExpression[Expression] = SingleQueryExpression(lit42)

    val nProp = prop("n", "prop")
    val nPropEqualsXProp = Equals(nProp, xProp)(pos)
    val nPropEquals = equals(nProp, lit42)

    val nFoo = prop("n", "foo")
    val nFooEqualsYProp = Equals(nFoo, yProp)(pos)
    val nFooEquals = equals(nFoo, lit42)

    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit6, CTInteger.invariant)
      addTypeToSemanticTable(nProp, CTInteger.invariant)
      addTypeToSemanticTable(nFoo, CTInteger.invariant)
      val predicates: Set[Expression] = Set(
        hasLabels("n", "Awesome"),
        hasLabels("x", "Awesome"),
        nPropEquals,
        nPropEqualsXProp,
        nFooEquals,
        nFooEqualsYProp
      )

      qg = QueryGraph(
        selections = Selections(predicates.flatMap(_.asPredicates)),
        patternNodes = Set(v"n", v"x", v"y"),
        argumentIds = Set(v"x", v"y")
      )

      indexOn("Awesome", "prop", "foo")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val restriction = LeafPlanRestrictions.OnlyIndexSeekPlansFor(v"n", Set(v"x", v"y"))
      val resultPlans = indexSeekLeafPlanner(restriction)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val labelToken = LabelToken("Awesome", LabelId(0))
      val nPropToken = PropertyKeyToken("prop", PropertyKeyId(0))
      val nFooToken = PropertyKeyToken("foo", PropertyKeyId(1))
      val indexedProperties =
        Seq(IndexedProperty(nPropToken, CanGetValue, NODE_TYPE), IndexedProperty(nFooToken, CanGetValue, NODE_TYPE))

      // This contains all combinations except n.prop = 42 AND n.foo = 42, because it does not depend on x or y
      val expected = Set(
        NodeIndexSeek(
          n,
          labelToken,
          indexedProperties,
          CompositeQueryExpression(Seq(xPropExpr, lit42Expr)),
          Set(v"x", v"y"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          indexedProperties,
          CompositeQueryExpression(Seq(xPropExpr, yPropExpr)),
          Set(v"x", v"y"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          indexedProperties,
          CompositeQueryExpression(Seq(lit42Expr, yPropExpr)),
          Set(v"x", v"y"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        )
      )

      resultPlans shouldEqual expected
    }
  }

  test("plans index plans for unrestricted variables") {
    val nProp = prop("n", "prop")
    val xProp = prop("x", "prop")
    val nPropEqualsXProp = Equals(nProp, xProp)(pos)
    val xPropExpr = SingleQueryExpression(xProp)
    val nPropEquals = equals(lit42, nProp)
    val nPropIn = in(nProp, listOf(lit6, lit42))
    val nPropLessThan = AndedPropertyInequalities(v"n", nProp, NonEmptyList(lessThan(nProp, lit6)))
    val literalFoo = literalString("foo")
    val nPropStartsWith = startsWith(nProp, literalFoo)
    val nPropEndsWith = endsWith(nProp, literalFoo)
    val nPropContains = contains(nProp, literalFoo)
    val point = function("point", mapOfInt("x" -> 1, "y" -> 2))
    val nPropDistance = greaterThan(lit42, function(List("point"), "distance", nProp, point))
    new givenConfig {
      addTypeToSemanticTable(nProp, CTInteger.invariant)
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      addTypeToSemanticTable(lit6, CTInteger.invariant)

      val predicates: Set[Expression] = Set(
        hasLabels("n", "Awesome"),
        hasLabels("m", "Awesome"),
        hasLabels("x", "Awesome"),
        nPropIn,
        nPropLessThan,
        nPropEquals,
        nPropStartsWith,
        nPropEndsWith,
        nPropContains,
        nPropDistance,
        nPropEqualsXProp
      )

      qg = QueryGraph(
        selections = Selections(predicates.flatMap(_.asPredicates)),
        patternNodes = Set(v"n", v"m", v"x"),
        argumentIds = Set(v"x")
      )

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val restriction = LeafPlanRestrictions.OnlyIndexSeekPlansFor(v"m", Set(v"x"))
      val resultPlans = indexSeekLeafPlanner(restriction)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      val labelToken = LabelToken("Awesome", LabelId(0))
      val nPropToken = PropertyKeyToken("prop", PropertyKeyId(0))

      val expected: Set[LogicalPlan] = Set(
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(nPropToken, CanGetValue, NODE_TYPE)),
          ManyQueryExpression(listOf(lit6, lit42)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(nPropToken, DoNotGetValue, NODE_TYPE)),
          RangeQueryExpression(InequalitySeekRangeWrapper(RangeLessThan(NonEmptyList(ExclusiveBound(lit6))))(pos)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(nPropToken, CanGetValue, NODE_TYPE)),
          SingleQueryExpression(lit42),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(nPropToken, DoNotGetValue, NODE_TYPE)),
          RangeQueryExpression(PrefixSeekRangeWrapper(PrefixRange(literalFoo))(pos)),
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        ),
        NodeIndexSeek(
          n,
          labelToken,
          Seq(IndexedProperty(nPropToken, CanGetValue, NODE_TYPE)),
          xPropExpr,
          Set(v"x"),
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        )
      )

      resultPlans shouldEqual expected
    }
  }

  test("unique index seek with values (equality predicate) when there is an unique index on the property") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, hasLabel("Awesome"))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeUniqueIndexSeek(
            `n`,
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            SingleQueryExpression(`lit42`),
            _,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("unique index seek without values when there is an index on the property") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropLessThanLit42, hasLabel("Awesome"))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeUniqueIndexSeek(
            `n`,
            _,
            Seq(IndexedProperty(_, DoNotGetValue, NODE_TYPE)),
            _,
            _,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("unique index seek with values (from index) when there is an index on the property") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropLessThanLit42, hasLabel("Awesome"))

      uniqueIndexOn("Awesome", "prop").providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeUniqueIndexSeek(
            `n`,
            _,
            Seq(IndexedProperty(_, CanGetValue, NODE_TYPE)),
            _,
            _,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("plans index seeks such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(v"n", labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, hasLabel("Awesome")).addHints(Some(hint))

      indexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexSeek(`n`, _, _, SingleQueryExpression(`lit42`), _, _, _, _)) => ()
      }

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldEqual Set(hint)
    }
  }

  test("plans unique index seeks such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(v"n", labelOrRelTypeName("Awesome"), Seq(PropertyKeyName("prop")(pos))) _

    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, hasLabel("Awesome")).addHints(Some(hint))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeUniqueIndexSeek(
            `n`,
            _,
            _,
            SingleQueryExpression(`lit42`),
            _,
            _,
            _,
            _
          )) => ()
      }

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldEqual Set(hint)
    }
  }

  test("plans merge unique index seeks when there are two unique indexes") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, hasLabel("Awesome"), hasLabel("Awesomer"))

      uniqueIndexOn("Awesome", "prop")
      uniqueIndexOn("Awesomer", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = mergeNodeUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(AssertSameNode(
            `n`,
            NodeUniqueIndexSeek(
              `n`,
              LabelToken("Awesome", _),
              _,
              SingleQueryExpression(`lit42`),
              _,
              _,
              _,
              _
            ),
            NodeUniqueIndexSeek(
              `n`,
              LabelToken("Awesomer", _),
              _,
              SingleQueryExpression(`lit42`),
              _,
              _,
              _,
              _
            )
          )) => ()
      }
    }
  }

  test("plans merge unique index seeks when there are only one unique index") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, hasLabel("Awesome"), hasLabel("Awesomer"))

      uniqueIndexOn("Awesome", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = mergeNodeUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeUniqueIndexSeek(
            `n`,
            _,
            _,
            SingleQueryExpression(`lit42`),
            _,
            _,
            _,
            _
          )) => ()
      }
    }
  }

  test("plans merge unique index seeks with AssertSameNode when there are three unique indexes") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, hasLabel("Awesome"), hasLabel("Awesomer"), hasLabel("Awesomest"))

      uniqueIndexOn("Awesome", "prop")
      uniqueIndexOn("Awesomer", "prop")
      uniqueIndexOn("Awesomest", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = mergeNodeUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(
            AssertSameNode(
              `n`,
              AssertSameNode(
                `n`,
                NodeUniqueIndexSeek(
                  `n`,
                  LabelToken(l1, _),
                  _,
                  SingleQueryExpression(`lit42`),
                  _,
                  _,
                  _,
                  _
                ),
                NodeUniqueIndexSeek(
                  `n`,
                  LabelToken(l2, _),
                  _,
                  SingleQueryExpression(`lit42`),
                  _,
                  _,
                  _,
                  _
                )
              ),
              NodeUniqueIndexSeek(
                `n`,
                LabelToken(l3, _),
                _,
                SingleQueryExpression(`lit42`),
                _,
                _,
                _,
                _
              )
            )
          ) if Set(l1, l2, l3) == Set("Awesome", "Awesomer", "Awesomest") => ()
      }
    }
  }

  test("plans merge unique index seeks with AssertSameNode when there are four unique indexes") {
    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(
        nPropInLit42,
        hasLabel("Awesome"),
        hasLabel("Awesomer"),
        hasLabel("Awesomest"),
        hasLabel("Awesomestest")
      )

      uniqueIndexOn("Awesome", "prop")
      uniqueIndexOn("Awesomer", "prop")
      uniqueIndexOn("Awesomest", "prop")
      uniqueIndexOn("Awesomestest", "prop")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = mergeNodeUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(
            AssertSameNode(
              `n`,
              AssertSameNode(
                `n`,
                AssertSameNode(
                  `n`,
                  NodeUniqueIndexSeek(
                    `n`,
                    LabelToken(l1, _),
                    _,
                    SingleQueryExpression(`lit42`),
                    _,
                    _,
                    _,
                    _
                  ),
                  NodeUniqueIndexSeek(
                    `n`,
                    LabelToken(l2, _),
                    _,
                    SingleQueryExpression(`lit42`),
                    _,
                    _,
                    _,
                    _
                  )
                ),
                NodeUniqueIndexSeek(
                  `n`,
                  LabelToken(l3, _),
                  _,
                  SingleQueryExpression(`lit42`),
                  _,
                  _,
                  _,
                  _
                )
              ),
              NodeUniqueIndexSeek(
                `n`,
                LabelToken(l4, _),
                _,
                SingleQueryExpression(`lit42`),
                _,
                _,
                _,
                _
              )
            )
          ) if Set(l1, l2, l3, l4) == Set("Awesome", "Awesomer", "Awesomest", "Awesomestest") => ()
      }
    }
  }

  test("test with three predicates, a single prop constraint and a two-prop constraint") {
    // MERGE (a:X {prop1: 42, prop2: 444, prop3: 56})
    // Unique constraint on :X(prop1, prop2)
    // Unique constraint on :X(prop3)

    val val1 = literalInt(44)
    val val2 = literalInt(55)
    val val3 = literalInt(66)
    val pred1 = equals(prop("n", "prop1"), val1)
    val pred2 = equals(prop("n", "prop2"), val2)
    val pred3 = equals(prop("n", "prop3"), val3)
    new givenConfig {
      addTypeToSemanticTable(val1, CTInteger.invariant)
      addTypeToSemanticTable(val2, CTInteger.invariant)
      addTypeToSemanticTable(val3, CTInteger.invariant)
      qg = queryGraph(pred1, pred2, pred3, hasLabel("Awesome"))

      uniqueIndexOn("Awesome", "prop1", "prop2")
      uniqueIndexOn("Awesome", "prop3")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = mergeNodeUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(
            AssertSameNode(
              `n`,
              NodeUniqueIndexSeek(
                `n`,
                LabelToken("Awesome", _),
                Seq(
                  IndexedProperty(PropertyKeyToken("prop1", _), CanGetValue, NODE_TYPE),
                  IndexedProperty(PropertyKeyToken("prop2", _), CanGetValue, NODE_TYPE)
                ),
                CompositeQueryExpression(Seq(
                  SingleQueryExpression(`val1`),
                  SingleQueryExpression(`val2`)
                )),
                _,
                _,
                _,
                _
              ),
              NodeUniqueIndexSeek(
                `n`,
                LabelToken("Awesome", _),
                _,
                SingleQueryExpression(`val3`),
                _,
                _,
                _,
                _
              )
            )
          ) => ()
      }
    }
  }

  test("test with three predicates, two composite two-prop constraints") {
    // MERGE (a:X {prop1: 42, prop2: 444, prop3: 56})
    // Unique constraint on :X(prop1, prop2)
    // Unique constraint on :X(prop2, prop3)

    val val1 = literalInt(44)
    val val2 = literalInt(55)
    val val3 = literalInt(66)
    val pred1 = equals(prop("n", "prop1"), val1)
    val pred2 = equals(prop("n", "prop2"), val2)
    val pred3 = equals(prop("n", "prop3"), val3)
    new givenConfig {
      addTypeToSemanticTable(val1, CTInteger.invariant)
      addTypeToSemanticTable(val2, CTInteger.invariant)
      addTypeToSemanticTable(val3, CTInteger.invariant)
      qg = queryGraph(pred1, pred2, pred3, hasLabel("Awesome"))

      uniqueIndexOn("Awesome", "prop1", "prop2")
      uniqueIndexOn("Awesome", "prop2", "prop3")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = mergeNodeUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(
            AssertSameNode(
              `n`,
              NodeUniqueIndexSeek(
                `n`,
                LabelToken("Awesome", _),
                Seq(
                  IndexedProperty(PropertyKeyToken("prop1", _), CanGetValue, NODE_TYPE),
                  IndexedProperty(PropertyKeyToken("prop2", _), CanGetValue, NODE_TYPE)
                ),
                CompositeQueryExpression(Seq(
                  SingleQueryExpression(`val1`),
                  SingleQueryExpression(`val2`)
                )),
                _,
                _,
                _,
                _
              ),
              NodeUniqueIndexSeek(
                `n`,
                LabelToken("Awesome", _),
                Seq(
                  IndexedProperty(PropertyKeyToken("prop2", _), CanGetValue, NODE_TYPE),
                  IndexedProperty(PropertyKeyToken("prop3", _), CanGetValue, NODE_TYPE)
                ),
                CompositeQueryExpression(Seq(
                  SingleQueryExpression(`val2`),
                  SingleQueryExpression(`val3`)
                )),
                _,
                _,
                _,
                _
              )
            )
          ) => ()
      }
    }
  }

  test("test with three predicates, single composite three-prop constraints") {
    // MERGE (a:X {prop1: 42, prop2: 444, prop3: 56})
    // Unique constraint on :X(prop1, prop2, prop3)

    val val1 = literalInt(44)
    val val2 = literalInt(55)
    val val3 = literalInt(66)
    val pred1 = equals(prop("n", "prop1"), val1)
    val pred2 = equals(prop("n", "prop2"), val2)
    val pred3 = equals(prop("n", "prop3"), val3)
    new givenConfig {
      addTypeToSemanticTable(val1, CTInteger.invariant)
      addTypeToSemanticTable(val2, CTInteger.invariant)
      addTypeToSemanticTable(val3, CTInteger.invariant)
      qg = queryGraph(pred1, pred2, pred3, hasLabel("Awesome"))

      uniqueIndexOn("Awesome", "prop1", "prop2", "prop3")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = mergeNodeUniqueIndexSeekLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(
            NodeUniqueIndexSeek(
              `n`,
              LabelToken("Awesome", _),
              Seq(
                IndexedProperty(PropertyKeyToken("prop1", _), CanGetValue, NODE_TYPE),
                IndexedProperty(PropertyKeyToken("prop2", _), CanGetValue, NODE_TYPE),
                IndexedProperty(PropertyKeyToken("prop3", _), CanGetValue, NODE_TYPE)
              ),
              CompositeQueryExpression(Seq(
                SingleQueryExpression(`val1`),
                SingleQueryExpression(`val2`),
                SingleQueryExpression(`val3`)
              )),
              _,
              _,
              _,
              _
            )
          ) => ()
      }
    }
  }

  test("should not plan using implicit IS NOT NULL if explicit IS NOT NULL exists") {

    new givenConfig {
      addTypeToSemanticTable(lit42, CTInteger.invariant)
      qg = queryGraph(nPropInLit42, nFooIsNotNull, hasLabel("Awesome"))

      nodePropertyExistenceConstraintOn("Awesome", Set("prop", "foo"))
      indexOn("Awesome", "prop", "foo")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(p @ NodeIndexSeek(
            `n`,
            _,
            _,
            CompositeQueryExpression(Seq(SingleQueryExpression(`lit42`), ExistenceQueryExpression())),
            _,
            _,
            _,
            _
          )) =>
          val plannedQG = ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
          plannedQG.selections.flatPredicates.toSet shouldEqual Set(
            nPropInLit42,
            nFooIsNotNull,
            hasLabel("Awesome")
          )
      }

      // We should not consider solutions that use an implicit n.foo IS NOT NULL, since we have one explicitly in the query
      // Otherwise we risk mixing up the solveds, since the plans would be exactly the same
      val implicitIsNotNullSolutions = ctx.staticComponents.planningAttributes.solveds.toSeq
        .filter(_.hasValue)
        .map(_.value.asSinglePlannerQuery.queryGraph.selections.flatPredicatesSet)
        .filter(_ == Set(nPropInLit42, hasLabel("Awesome")))

      implicitIsNotNullSolutions shouldEqual Seq.empty
    }
  }

  test("should plan seek with ENDS WITH as existence after equality") {

    new givenConfig {
      indexOn("Awesome", "foo", "prop")

      qg = queryGraph(nFooEqualsLit42, nPropEndsWithLitText, hasLabel("Awesome"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexSeek(
            `n`,
            _,
            _,
            CompositeQueryExpression(Seq(SingleQueryExpression(`lit42`), ExistenceQueryExpression())),
            _,
            _,
            _,
            _
          )) =>
      }
    }
  }

  test("should plan seek with CONTAINS as existence after equality") {

    new givenConfig {
      indexOn("Awesome", "foo", "prop")

      qg = queryGraph(nFooEqualsLit42, nPropContainsLitText, hasLabel("Awesome"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexSeek(
            `n`,
            _,
            _,
            CompositeQueryExpression(Seq(SingleQueryExpression(`lit42`), ExistenceQueryExpression())),
            _,
            _,
            _,
            _
          )) =>
      }
    }
  }

  test("should plan seek with Regex as existence after equality") {

    new givenConfig {
      indexOn("Awesome", "foo", "prop")

      qg = queryGraph(nFooEqualsLit42, regex(nProp, literalString("Text")), hasLabel("Awesome"))
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        indexSeekLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case SetExtractor(NodeIndexSeek(
            `n`,
            _,
            _,
            CompositeQueryExpression(Seq(SingleQueryExpression(`lit42`), ExistenceQueryExpression())),
            _,
            _,
            _,
            _
          )) =>
      }
    }
  }

  private def queryGraph(predicates: Expression*) =
    QueryGraph(
      selections = Selections(predicates.map(Predicate(Set(n), _)).toSet),
      patternNodes = Set(n)
    )
}
