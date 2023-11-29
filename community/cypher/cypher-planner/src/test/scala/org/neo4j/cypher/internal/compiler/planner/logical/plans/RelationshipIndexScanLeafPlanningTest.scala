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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.SeekOnly
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexStringSearchScanPlanProvider
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class RelationshipIndexScanLeafPlanningTest extends CypherFunSuite with LogicalPlanningTestSupport2
    with AstConstructionTestSupport {

  private val relVar = v"r"
  private val dependency = "dep"
  private val startNode = v"start"
  private val endNode = v"end"
  private val prop = "prop"
  private val foo = "foo"
  private val bar = "bar"
  private val baz = "baz"
  private val apa = "Apa"
  private val bepa = "Bepa"
  private val relTypeName = "REL"

  private val lit12 = literalInt(12)
  private val litApa = literalString(apa)
  private val litBepa = literalString(bepa)
  private val dependencyProp = prop(dependency, "prop")

  private val propIsNotNull = isNotNull(prop(relVar, prop))
  private val propStartsWithEmpty = startsWith(prop(relVar, prop), literalString(""))
  private val propEquals12 = equals(prop(relVar, prop), lit12)
  private val propContainsApa = contains(prop(relVar, prop), litApa)
  private val propContainsDepencencyProp = contains(prop(relVar, prop), dependencyProp)
  private val propContainsBepa = contains(prop(relVar, prop), litBepa)
  private val propEndsWithApa = endsWith(prop(relVar, prop), litApa)
  private val propEndsWithDependencyProp = endsWith(prop(relVar, prop), dependencyProp)
  private val propEndsWithBepa = endsWith(prop(relVar, prop), litBepa)
  private val propLessThan12 = lessThan(prop(relVar, prop), lit12)
  private val propNotEquals12 = not(equals(prop(relVar, prop), lit12))
  private val propRegexMatchJohnny = regex(prop(relVar, prop), literalString("Johnny"))

  private val fooIsNotNull = isNotNull(prop(relVar, foo))
  private val fooContainsApa = contains(prop(relVar, foo), litApa)
  private val barIsNotNull = isNotNull(prop(relVar, bar))
  private val bazIsNotNull = isNotNull(prop(relVar, baz))
  private val bazEquals12 = equals(prop(relVar, baz), lit12)

  private def relationshipIndexScanLeafPlanner(restrictions: LeafPlanRestrictions) =
    RelationshipIndexLeafPlanner(Seq(RelationshipIndexScanPlanProvider), restrictions)

  private def relationshipIndexStringSearchScanLeafPlanner(restrictions: LeafPlanRestrictions) =
    RelationshipIndexLeafPlanner(Seq(RelationshipIndexStringSearchScanPlanProvider), restrictions)

  private def solvedPredicates(plan: LogicalPlan, ctx: LogicalPlanningContext) =
    ctx.staticComponents.planningAttributes.solveds.get(
      plan.id
    ).asSinglePlannerQuery.queryGraph.selections.predicates.map(_.expr)

  private def queryGraph(types: Seq[String], semanticDirection: SemanticDirection, predicates: Expression*) =
    QueryGraph(
      selections = Selections(predicates.map(Predicate(Set(relVar), _)).toSet),
      patternRelationships = Set(PatternRelationship(
        relVar,
        (startNode, endNode),
        semanticDirection,
        types.map(super[AstConstructionTestSupport].relTypeName(_)),
        SimplePatternLength
      )),
      argumentIds = Set(dependency)
    )

  test("does not plan index scan when no index exist") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propIsNotNull)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("index scan when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
    }
  }

  test("index scan when there is an index on the property for when matching on incoming relationship") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), INCOMING, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)<-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
    }
  }

  test("index scan when there is an index on the property for when matching on outgoing relationship") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), OUTGOING, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]->($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
    }
  }

  test("no index scan when there is an index on the property but relationship variable is skipped") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.OnlyIndexSeekPlansFor(relVar.name, Set(dependency)))(
          cfg.qg,
          InterestingOrderConfig.empty,
          ctx
        )

      // then
      resultPlans should be(empty)
    }
  }

  test("index scan solves is not null") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propIsNotNull
      ))
    }
  }

  test("index scan for equality solves is not null") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propEquals12, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propIsNotNull
      ))
    }
  }

  test("index scan with values when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propIsNotNull)
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.RANGE
          )
          .build()
      ))
    }
  }

  test("plans index scans such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(relVar, labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos))) _

    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propIsNotNull).addHints(Some(hint))
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      ctx.staticComponents.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.queryGraph.hints should equal(
        Set(hint)
      )
    }
  }

  test("index scan does not solve seek hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(relVar, labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos)), SeekOnly) _

    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propIsNotNull).addHints(Some(hint))
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      ctx.staticComponents.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.queryGraph.hints should be(
        empty
      )
    }
  }

  test("plans index scans for: n.prop STARTS WITH <pattern>") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propStartsWithEmpty)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propIsNotNull, propStartsWithEmpty)
      ))
    }
  }

  test("plans index scans with value for: n.prop STARTS WITH <pattern>") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propStartsWithEmpty)
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propIsNotNull, propStartsWithEmpty)
      ))
    }
  }

  test("plans index scans for: n.prop < <value>") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propLessThan12)
      addTypeToSemanticTable(lit12, CTInteger.invariant)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propIsNotNull, propLessThan12)
      ))
    }
  }

  test("plans index scans for: n.prop <> <value>") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propNotEquals12)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propIsNotNull, propNotEquals12)
      ))
    }
  }

  test("plans index scans for: n.prop = <value>") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propEquals12)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propIsNotNull, propEquals12)
      ))
    }
  }

  test("plans index scans for: n.prop = <pattern>") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propRegexMatchJohnny)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propIsNotNull, propRegexMatchJohnny)
      ))
    }
  }

  test("plans composite index scans when there is a composite index and multiple predicates") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, fooContainsApa, propContainsApa, barIsNotNull, bazEquals12)
      relationshipIndexOn(relTypeName, foo, prop, bar, baz)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($foo, $prop, $bar, $baz)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propIsNotNull, propContainsApa),
        PartialPredicate(fooIsNotNull, fooContainsApa),
        barIsNotNull,
        PartialPredicate(bazIsNotNull, bazEquals12)
      ))
    }
  }

  test("plans composite index scans when there is a composite index and multiple predicates on the same property") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa, propEquals12, fooIsNotNull, barIsNotNull, bazEquals12)
      relationshipIndexOn(relTypeName, foo, prop, bar, baz)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($foo, $prop, $bar, $baz)]-($endNode)".name,
            argumentIds = Set(dependency),
            indexType = IndexType.RANGE
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propIsNotNull, propContainsApa), // missing
        PartialPredicate(propIsNotNull, propEquals12),
        fooIsNotNull,
        barIsNotNull,
        PartialPredicate(bazIsNotNull, bazEquals12)
      ))
    }
  }

  test("plans no composite index scans when there is a composite index but not enough predicates") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa, fooContainsApa, barIsNotNull)
      relationshipIndexOn(relTypeName, foo, prop, bar, baz)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should be(empty)
    }
  }

  // INDEX CONTAINS SCAN

  test("does not plan index contains scan when no index exist") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans shouldBe empty
    }
  }

  test("does plan index contains scan when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa)
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop CONTAINS '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))
    }
  }

  test("does plan directed index contains scan when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), OUTGOING, propContainsApa)
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop CONTAINS '$apa')]->($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))
    }
  }

  test("index contains scan with values when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa)
      relationshipTextIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop CONTAINS '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))
    }
  }

  test("multiple index contains scans with values when there is an index on the property and multiple predicates") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), INCOMING, propContainsApa, propContainsBepa)
      relationshipTextIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)<-[$relVar:$relTypeName($prop CONTAINS '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.TEXT
          )
          .build(),
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)<-[$relVar:$relTypeName($prop CONTAINS '$bepa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))

      solvedPredicates(resultPlans.head, ctx) should equal(Set(
        propContainsApa
      ))
      solvedPredicates(resultPlans.tail.head, ctx) should equal(Set(
        propContainsBepa
      ))
    }
  }

  test("no index contains scans with values when there is a composite index on the property and multiple predicates") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa, propContainsApa)
      relationshipIndexOn(relTypeName, prop, foo).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans shouldBe empty
    }
  }

  test("plans index contains scans such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(relVar, labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos))) _

    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa).addHints(Some(hint))
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop CONTAINS '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldEqual Set(hint)
    }
  }

  test("index contains scan does not solve seek hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(relVar, labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos)), SeekOnly) _

    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa).addHints(Some(hint))
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop CONTAINS '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldBe empty
    }
  }

  test("index contains scan when there is an index on the property but relationship variable is restricted") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsDepencencyProp)
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.OnlyIndexSeekPlansFor(
          relVar.name,
          Set(dependency)
        ))(
          cfg.qg,
          InterestingOrderConfig.empty,
          ctx
        )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop CONTAINS ???)]-($endNode)".name,
            paramExpr = Seq(dependencyProp),
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
    }
  }

  // INDEX ENDS WITH SCAN

  test("does not plan index ends with scan when no index exist") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans shouldBe empty
    }
  }

  test("does plan index ends with scan when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa)
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop ENDS WITH '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))
    }
  }

  test("does plan directed index ends with scan when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), OUTGOING, propEndsWithApa)
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop ENDS WITH '$apa')]->($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))
    }
  }

  test("index ends with scan with values when there is an index on the property") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa)
      relationshipTextIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop ENDS WITH '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))
    }
  }

  test("multiple index ends with scans with values when there is an index on the property and multiple predicates") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), INCOMING, propEndsWithApa, propEndsWithBepa)
      relationshipTextIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)<-[$relVar:$relTypeName($prop ENDS WITH '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.TEXT
          )
          .build(),
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)<-[$relVar:$relTypeName($prop ENDS WITH '$bepa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => CanGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      solvedPredicates(resultPlans.head, ctx) should equal(Set(
        propEndsWithApa
      ))
      solvedPredicates(resultPlans.tail.head, ctx) should equal(Set(
        propEndsWithBepa
      ))
    }
  }

  test("no index ends with scans with values when there is a composite index on the property and multiple predicates") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa, propEndsWithApa)
      relationshipIndexOn(relTypeName, prop, foo).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans shouldBe empty
    }
  }

  test("plans index ends with scans such that it solves hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(relVar, labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos))) _

    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa).addHints(Some(hint))
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop ENDS WITH '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldEqual Set(hint)
    }
  }

  test("index ends with scan does not solve seek hints") {
    val hint: UsingIndexHint =
      UsingIndexHint(relVar, labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos)), SeekOnly) _

    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa).addHints(Some(hint))
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(
        cfg.qg,
        InterestingOrderConfig.empty,
        ctx
      )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop ENDS WITH '$apa')]-($endNode)".name,
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))

      resultPlans.map(p =>
        ctx.staticComponents.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph
      ).head.hints shouldBe empty
    }
  }

  test("index ends with scan when there is an index on the property but relationship variable is restricted") {
    new givenConfig {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithDependencyProp)
      relationshipTextIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans =
        relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.OnlyIndexSeekPlansFor(
          relVar.name,
          Set(dependency)
        ))(
          cfg.qg,
          InterestingOrderConfig.empty,
          ctx
        )

      // then
      resultPlans should equal(Set(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(
            v"($startNode)-[$relVar:$relTypeName($prop ENDS WITH ???)]-($endNode)".name,
            paramExpr = Seq(dependencyProp),
            argumentIds = Set(dependency),
            getValue = _ => DoNotGetValue,
            indexType = IndexType.TEXT
          )
          .build()
      ))

    }
  }
}
