/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.SeekOnly
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
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
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RelationshipIndexScanLeafPlanningTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  private val relName = "r"
  private val startNodeName = "start"
  private val endNodeName = "end"
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

  private val propExists = function(Exists.name, prop(relName, prop))
  private val propIsNotNull = isNotNull(prop(relName, prop))
  private val propStartsWithEmpty = startsWith(prop(relName, prop), literalString(""))
  private val propEquals12 = equals(prop(relName, prop), lit12)
  private val propContainsApa = contains(prop(relName, prop), litApa)
  private val propContainsBepa = contains(prop(relName, prop), litBepa)
  private val propEndsWithApa = endsWith(prop(relName, prop), litApa)
  private val propEndsWithBepa = endsWith(prop(relName, prop), litBepa)
  private val propLessThan12 = lessThan(prop(relName, prop), lit12)
  private val propNotEquals12 = notEquals(prop(relName, prop), lit12)
  private val propRegexMatchJohnny = regex(prop(relName, prop), literalString("Johnny"))

  private val fooExists = function(Exists.name, prop(relName, foo))
  private val fooContainsApa = contains(prop(relName, foo), litApa)
  private val barExists = function(Exists.name, prop(relName, bar))
  private val bazExists = function(Exists.name, prop(relName, baz))
  private val bazEquals12 = equals(prop(relName, baz), lit12)

  private def relationshipIndexScanLeafPlanner(restrictions: LeafPlanRestrictions) =
    RelationshipIndexLeafPlanner(Seq(RelationshipIndexScanPlanProvider()), restrictions)

  private def relationshipIndexStringSearchScanLeafPlanner(restrictions: LeafPlanRestrictions) =
    RelationshipIndexLeafPlanner(Seq(RelationshipIndexStringSearchScanPlanProvider), restrictions)

  private def solvedPredicates(plan: LogicalPlan, ctx: LogicalPlanningContext) =
    ctx.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.queryGraph.selections.predicates.map(_.expr)

  private def queryGraph(types: Seq[String], semanticDirection: SemanticDirection, predicates: Expression*) =
    QueryGraph(
      selections = Selections(predicates.map(Predicate(Set(relName), _)).toSet),
      patternRelationships = Set(PatternRelationship(
        relName,
        (startNodeName, endNodeName),
        semanticDirection,
        types.map(super[AstConstructionTestSupport].relTypeName),
        SimplePatternLength))
    )

  test("does not plan index scan when no index exist") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propExists)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("index scan when there is an index on the property") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propExists)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
    }
  }

  test("index scan when there is an index on the property for when matching on incoming relationship") {
    new given {
      qg = queryGraph(Seq(relTypeName), INCOMING, propExists)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)<-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
    }
  }

  test("index scan when there is an index on the property for when matching on outgoing relationship") {
    new given {
      qg = queryGraph(Seq(relTypeName), OUTGOING, propExists)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]->($endNodeName)")
          .build()
      ))
    }
  }

  test("no index scan when there is an index on the property but relationship variable is skipped") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propExists)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.OnlyIndexSeekPlansFor(relName, Set()))(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should be(empty)
    }
  }

  test("index scan solves is not null") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propIsNotNull,
      ))
    }
  }

  test("index scan solves both exists and is not null") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propExists, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propExists,
        propIsNotNull,
      ))
    }
  }

  test("index scan for equality solves exists") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEquals12, propExists)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propExists,
      ))
    }
  }

  test("index scan for equality solves is not null") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEquals12, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propIsNotNull,
      ))
    }
  }

  test("index scan for equality solves both exists and is not null") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEquals12, propExists, propIsNotNull)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propExists,
        propIsNotNull,
      ))
    }
  }

  test("index scan with values when there is an index on the property") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propExists)
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)", getValue = _ => CanGetValue)
          .build()
      ))
    }
  }

  test("plans index scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor(relName), labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos))) _

    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propExists).addHints(Some(hint))
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)", getValue = _ => CanGetValue)
          .build()
      ))
      val plan = resultPlans.head
      ctx.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.queryGraph.hints should equal(Set(hint))
    }
  }

  test("index scan does not solve seek hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor(relName), labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos)), SeekOnly) _

    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propExists).addHints(Some(hint))
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)", getValue = _ => CanGetValue)
          .build()
      ))
      val plan = resultPlans.head
      ctx.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.queryGraph.hints should be(empty)
    }
  }

  test("plans index scans for: n.prop STARTS WITH <pattern>") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propStartsWithEmpty)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propExists, propStartsWithEmpty),
      ))
    }
  }

  test("plans index scans with value for: n.prop STARTS WITH <pattern>") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propStartsWithEmpty)
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)", getValue = _ => CanGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propExists, propStartsWithEmpty),
      ))
    }
  }

  test("plans index scans for: n.prop < <value>") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propLessThan12)
      addTypeToSemanticTable(lit12, CTInteger.invariant)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propExists, propLessThan12),
      ))
    }
  }

  test("plans index scans for: n.prop <> <value>") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propNotEquals12)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propExists, propNotEquals12),
      ))
    }
  }

  test("plans index scans for: n.prop = <value>") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEquals12)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propExists, propEquals12),
      ))
    }
  }

  test("plans index scans for: n.prop = <pattern>") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propRegexMatchJohnny)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propExists, propRegexMatchJohnny),
      ))
    }
  }

  test("plans composite index scans when there is a composite index and multiple predicates") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, fooContainsApa, propContainsApa, barExists, bazEquals12)
      relationshipIndexOn(relTypeName, foo, prop, bar, baz)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($foo, $prop, $bar, $baz)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propExists, propContainsApa),
        PartialPredicate(fooExists, fooContainsApa),
        barExists,
        PartialPredicate(bazExists, bazEquals12),
      ))
    }
  }

  test("plans composite index scans when there is a composite index and multiple predicates on the same property") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa, propEquals12, fooExists, barExists, bazEquals12)
      relationshipIndexOn(relTypeName, foo, prop, bar, baz)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($foo, $prop, $bar, $baz)]-($endNodeName)")
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        PartialPredicate(propExists, propContainsApa), // missing
        PartialPredicate(propExists, propEquals12),
        fooExists,
        barExists,
        PartialPredicate(bazExists, bazEquals12),
      ))
    }
  }

  test("plans no composite index scans when there is a composite index but not enough predicates") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa, fooContainsApa, barExists)
      relationshipIndexOn(relTypeName, foo, prop, bar, baz)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should be(empty)
    }
  }

  // INDEX CONTAINS SCAN

  test("does not plan index contains scan when no index exist") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does plan index contains scan when there is an index on the property") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop CONTAINS '$apa')]-($endNodeName)", getValue = _ => DoNotGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))
    }
  }

  test("does plan directed index contains scan when there is an index on the property") {
    new given {
      qg = queryGraph(Seq(relTypeName), OUTGOING, propContainsApa)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop CONTAINS '$apa')]->($endNodeName)", getValue = _ => DoNotGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))
    }
  }

  test("index contains scan with values when there is an index on the property") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa)
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop CONTAINS '$apa')]-($endNodeName)", getValue = _ => CanGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))
    }
  }

  test("multiple index contains scans with values when there is an index on the property and multiple predicates") {
    new given {
      qg = queryGraph(Seq(relTypeName), INCOMING, propContainsApa, propContainsBepa)
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)<-[$relName:$relTypeName($prop CONTAINS '$apa')]-($endNodeName)", getValue = _ => CanGetValue)
          .build(),
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)<-[$relName:$relTypeName($prop CONTAINS '$bepa')]-($endNodeName)", getValue = _ => CanGetValue)
          .build()
      ))

      solvedPredicates(resultPlans.head, ctx) should equal(Set(
        propContainsApa
      ))
      solvedPredicates(resultPlans(1), ctx) should equal(Set(
        propContainsBepa
      ))
    }
  }

  test("no index contains scans with values when there is a composite index on the property and multiple predicates") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa, propContainsApa)
      relationshipIndexOn(relTypeName, prop, foo).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq() => ()
      }
    }
  }

  test("plans index contains scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor(relName), labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos))) _

    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa).addHints(Some(hint))
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop CONTAINS '$apa')]-($endNodeName)", getValue = _ => DoNotGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Set(hint) => ()
      }
    }
  }

  test("index contains scan does not solve seek hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor(relName), labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos)), SeekOnly) _

    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa).addHints(Some(hint))
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop CONTAINS '$apa')]-($endNodeName)", getValue = _ => DoNotGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propContainsApa
      ))

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints.isEmpty => ()
      }
    }
  }

  test("no index contains scan when there is an index on the property but relationship variable is skipped") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propContainsApa)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.OnlyIndexSeekPlansFor(relName, Set()))(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should be(empty)
    }
  }

  // INDEX ENDS WITH SCAN

  test("does not plan index ends with scan when no index exist") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }

  test("does plan index ends with scan when there is an index on the property") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop ENDS WITH '$apa')]-($endNodeName)", getValue = _ => DoNotGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))
    }
  }

  test("does plan directed index ends with scan when there is an index on the property") {
    new given {
      qg = queryGraph(Seq(relTypeName), OUTGOING, propEndsWithApa)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop ENDS WITH '$apa')]->($endNodeName)", getValue = _ => DoNotGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))
    }
  }

  test("index ends with scan with values when there is an index on the property") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa)
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop ENDS WITH '$apa')]-($endNodeName)", getValue = _ => CanGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))
    }
  }

  test("multiple index ends with scans with values when there is an index on the property and multiple predicates") {
    new given {
      qg = queryGraph(Seq(relTypeName), INCOMING, propEndsWithApa, propEndsWithBepa)
      relationshipIndexOn(relTypeName, prop).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq(
        DirectedRelationshipIndexEndsWithScan(`relName`, `endNodeName`, `startNodeName`, _, IndexedProperty(_, CanGetValue, RELATIONSHIP_TYPE), `litApa`, _, _),
        DirectedRelationshipIndexEndsWithScan(`relName`, `endNodeName`, `startNodeName`, _, IndexedProperty(_, CanGetValue, RELATIONSHIP_TYPE), `litBepa`, _, _),
        ) => ()
      }
      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)<-[$relName:$relTypeName($prop ENDS WITH '$apa')]-($endNodeName)", getValue = _ => CanGetValue)
          .build(),
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)<-[$relName:$relTypeName($prop ENDS WITH '$bepa')]-($endNodeName)", getValue = _ => CanGetValue)
          .build()
      ))
      solvedPredicates(resultPlans.head, ctx) should equal(Set(
        propEndsWithApa
      ))
      solvedPredicates(resultPlans(1), ctx) should equal(Set(
        propEndsWithBepa
      ))
    }
  }

  test("no index ends with scans with values when there is a composite index on the property and multiple predicates") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa, propEndsWithApa)
      relationshipIndexOn(relTypeName, prop, foo).providesValues()
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should beLike {
        case Seq() => ()
      }
    }
  }

  test("plans index ends with scans such that it solves hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor(relName), labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos))) _

    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa).addHints(Some(hint))
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop ENDS WITH '$apa')]-($endNodeName)", getValue = _ => DoNotGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints == Set(hint) => ()
      }
    }
  }

  test("index ends with scan does not solve seek hints") {
    val hint: UsingIndexHint = UsingIndexHint(varFor(relName), labelOrRelTypeName(relTypeName), Seq(PropertyKeyName(prop)(pos)), SeekOnly) _

    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa).addHints(Some(hint))
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should equal(Seq(
        new LogicalPlanBuilder(wholePlan = false)
          .relationshipIndexOperator(s"($startNodeName)-[$relName:$relTypeName($prop ENDS WITH '$apa')]-($endNodeName)", getValue = _ => DoNotGetValue)
          .build()
      ))
      val plan = resultPlans.head
      solvedPredicates(plan, ctx) should equal(Set(
        propEndsWithApa
      ))

      resultPlans.map(p => ctx.planningAttributes.solveds.get(p.id).asSinglePlannerQuery.queryGraph) should beLike {
        case Seq(plannedQG: QueryGraph) if plannedQG.hints.isEmpty => ()
      }
    }
  }

  test("no index ends with scan when there is an index on the property but relationship variable is skipped") {
    new given {
      qg = queryGraph(Seq(relTypeName), BOTH, propEndsWithApa)
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexStringSearchScanLeafPlanner(LeafPlanRestrictions.OnlyIndexSeekPlansFor(relName, Set()))(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans should be(empty)
    }
  }

  test("should not plan relationship index scan for self-loop") {
    new given {
      qg = QueryGraph(
        selections = Selections(Set(Predicate(Set(relName), propIsNotNull))),
        patternRelationships = Set(PatternRelationship(
          relName,
          (startNodeName, startNodeName),
          BOTH,
          Seq(relTypeName(relTypeName)),
          SimplePatternLength)))
      relationshipIndexOn(relTypeName, prop)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = relationshipIndexScanLeafPlanner(LeafPlanRestrictions.NoRestrictions)(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldBe empty
    }
  }
}
