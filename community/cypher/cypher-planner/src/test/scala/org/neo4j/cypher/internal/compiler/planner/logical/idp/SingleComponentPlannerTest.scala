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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SingleComponentPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val noQPPInnerPlans = new CacheBackedQPPInnerPlanner(???)

  def planBuilder() = new LogicalPlanBuilder(wholePlan = false)

  private val `a-r1->a` = planBuilder()
    .expandInto("(a)-[r1]->(a)")
    .fakeLeafPlan("a")
    .build()

  private val `a-r1->b`: LogicalPlan = planBuilder()
    .expand("(a)-[r1]->(b)")
    .fakeLeafPlan("a")
    .build()

  private val `b<-r1-a`: LogicalPlan = planBuilder()
    .expand("(b)<-[r1]-(a)")
    .fakeLeafPlan("b")
    .build()

  private val `expandInto(a X b)` : LogicalPlan = planBuilder()
    .expandInto("(a)-[r1]->(b)")
    .cartesianProduct()
    .|.fakeLeafPlan("b")
    .fakeLeafPlan("a")
    .build()

  private val `expandInto(select(a X b))` : LogicalPlan = planBuilder()
    .expandInto("(a)-[r1]->(b)")
    .filter("a.prop = b.prop")
    .cartesianProduct()
    .|.fakeLeafPlan("b")
    .fakeLeafPlan("a")
    .build()

  private val `expandInto(b X a)` : LogicalPlan = planBuilder()
    .expandInto("(a)-[r1]->(b)")
    .cartesianProduct()
    .|.fakeLeafPlan("a")
    .fakeLeafPlan("b")
    .build()

  private val `expandInto(select(b X a))` : LogicalPlan = planBuilder()
    .expandInto("(a)-[r1]->(b)")
    .filter("a.prop = b.prop")
    .cartesianProduct()
    .|.fakeLeafPlan("a")
    .fakeLeafPlan("b")
    .build()

  private val `a-r1->b = b`: LogicalPlan = planBuilder()
    .nodeHashJoin("b")
    .|.fakeLeafPlan("b")
    .expand("(a)-[r1]->(b)")
    .fakeLeafPlan("a")
    .build()

  private val `b = a-r1->b`: LogicalPlan = planBuilder()
    .nodeHashJoin("b")
    .|.expand("(a)-[r1]->(b)")
    .|.fakeLeafPlan("a")
    .fakeLeafPlan("b")
    .build()

  private val `b<-r1-a = a`: LogicalPlan = planBuilder()
    .nodeHashJoin("a")
    .|.fakeLeafPlan("a")
    .expand("(b)<-[r1]-(a)")
    .fakeLeafPlan("b")
    .build()

  private val `a = b<-r1-a`: LogicalPlan = planBuilder()
    .nodeHashJoin("a")
    .|.expand("(b)<-[r1]-(a)")
    .|.fakeLeafPlan("b")
    .fakeLeafPlan("a")
    .build()

  private val `sort(a)-r1->b` = planBuilder()
    .expand("(a)-[r1]->(b)")
    .sort("1 ASC")
    .fakeLeafPlan("a")
    .build()

  private val `sort(b)<-r1-a` = planBuilder()
    .expand("(b)<-[r1]-(a)")
    .sort("1 ASC")
    .fakeLeafPlan("b")
    .build()

  private val `a-r1->b = sort(b)` = planBuilder()
    .nodeHashJoin("b")
    .|.sort("1 ASC")
    .|.fakeLeafPlan("b")
    .expand("(a)-[r1]->(b)")
    .fakeLeafPlan("a")
    .build()

  private val `b = sort(a)-r1->b` = planBuilder()
    .nodeHashJoin("b")
    .|.expand("(a)-[r1]->(b)")
    .|.sort("1 ASC")
    .|.fakeLeafPlan("a")
    .fakeLeafPlan("b")
    .build()

  private val `b<-r1-a = sort(a)` = planBuilder()
    .nodeHashJoin("a")
    .|.sort("1 ASC")
    .|.fakeLeafPlan("a")
    .expand("(b)<-[r1]-(a)")
    .fakeLeafPlan("b")
    .build()

  private val `a = sort(b)<-r1-a` = planBuilder()
    .nodeHashJoin("a")
    .|.expand("(b)<-[r1]-(a)")
    .|.sort("1 ASC")
    .|.fakeLeafPlan("b")
    .fakeLeafPlan("a")
    .build()

  private val `expandInto(sort(a) X b)` = planBuilder()
    .expandInto("(a)-[r1]->(b)")
    .cartesianProduct()
    .|.fakeLeafPlan("b")
    .sort("1 ASC")
    .fakeLeafPlan("a")
    .build()

  private val `expandInto(sort(b) X a)` = planBuilder()
    .expandInto("(a)-[r1]->(b)")
    .cartesianProduct()
    .|.fakeLeafPlan("a")
    .sort("1 ASC")
    .fakeLeafPlan("b")
    .build()

  private def mockContext() = {
    val planContext = mock[PlanContext]
    when(planContext.nodeTokenIndex).thenReturn(None)
    newMockedLogicalPlanningContext(planContext = planContext)
  }

  test("plans expands for queries with single pattern rel") {
    val pattern = PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set("a", "b"))
    val context = mockContext()
    val kit = context.plannerState.config.toKit(InterestingOrderConfig.empty, context)
    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")
    val bPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "b")

    // when
    val logicalPlans =
      SingleComponentPlanner.planSinglePattern(
        qg,
        kit,
        pattern,
        Map(
          aPlan.availableSymbols.map(_.name) -> BestResults(aPlan, None),
          bPlan.availableSymbols.map(_.name) -> BestResults(bPlan, None)
        ),
        noQPPInnerPlans,
        context
      )

    logicalPlans.toSet should equal(Set(`a-r1->b`, `b<-r1-a`, `expandInto(a X b)`, `expandInto(b X a)`))
  }

  test("plans expands with filter for queries with single pattern rel") {
    val pattern = PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(
      patternRelationships = Set(pattern),
      patternNodes = Set("a", "b"),
      selections = Selections.from(equals(prop("a", "prop"), prop("b", "prop")))
    )
    val context = mockContext()
    val kit = context.plannerState.config.toKit(InterestingOrderConfig.empty, context)
    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")
    val bPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "b")

    // when
    val logicalPlans =
      SingleComponentPlanner.planSinglePattern(
        qg,
        kit,
        pattern,
        Map(
          aPlan.availableSymbols.map(_.name) -> BestResults(aPlan, None),
          bPlan.availableSymbols.map(_.name) -> BestResults(bPlan, None)
        ),
        noQPPInnerPlans,
        context
      )

    // We only expect the filter to appear in the versions with a CartesianProduct.
    // The reason is that there we can filter before the ExpandInto.
    // For the ExpandAll plans, we will filter as the next step in SingleComponentPlanner.initTable anyway.
    logicalPlans.toSet should equal(Set(`a-r1->b`, `b<-r1-a`, `expandInto(select(a X b))`, `expandInto(select(b X a))`))
  }

  test("does not plan Expand on top of relationship leaf plan for queries with more than one pattern rel") {
    val rel1 = PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val rel2 = PatternRelationship(v"r2", (v"b", v"c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val qg = QueryGraph(patternRelationships = Set(rel1, rel2), patternNodes = Set("a", "b", "c"))
    val context = mockContext()
    val kit = context.plannerState.config.toKit(InterestingOrderConfig.empty, context)

    val r1Plan =
      newMockedLogicalPlanWithPatterns(
        context.staticComponents.planningAttributes,
        Set(rel1.variable, rel1.left, rel1.right).map(_.name),
        Set(rel1)
      )
    val r2Plan =
      newMockedLogicalPlanWithPatterns(
        context.staticComponents.planningAttributes,
        Set(rel2.variable, rel2.left, rel2.right).map(_.name),
        Set(rel2)
      )

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(
      qg,
      kit,
      rel1,
      Map(
        r1Plan.availableSymbols.map(_.name) -> BestResults(r1Plan, None),
        r2Plan.availableSymbols.map(_.name) -> BestResults(r2Plan, None)
      ),
      noQPPInnerPlans,
      context
    )

    // then
    logicalPlans.toSet should equal(Set(r1Plan))
  }

  test("plans hashjoins and cartesian product for queries with single pattern rel and multiple index hints") {
    val pattern = PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint1 = UsingIndexHint(varFor("a"), labelOrRelTypeName("X"), Seq(PropertyKeyName("p")(pos)))(pos)
    val hint2 = UsingIndexHint(varFor("b"), labelOrRelTypeName("X"), Seq(PropertyKeyName("p")(pos)))(pos)
    val qg =
      QueryGraph(patternRelationships = Set(pattern), patternNodes = Set("a", "b"), hints = Set(hint1, hint2))
    val context = mockContext()
    val kit = context.plannerState.config.toKit(InterestingOrderConfig.empty, context)

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")
    val bPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "b")

    // when
    val logicalPlans =
      SingleComponentPlanner.planSinglePattern(
        qg,
        kit,
        pattern,
        Map(
          aPlan.availableSymbols.map(_.name) -> BestResults(aPlan, None),
          bPlan.availableSymbols.map(_.name) -> BestResults(bPlan, None)
        ),
        noQPPInnerPlans,
        context
      )

    logicalPlans.toSet should equal(Set(
      `a-r1->b`,
      `b<-r1-a`,
      `a-r1->b = b`,
      `b = a-r1->b`,
      `b<-r1-a = a`,
      `a = b<-r1-a`,
      `expandInto(a X b)`,
      `expandInto(b X a)`
    ))
  }

  test("plans hashjoins and cartesian product for queries with single pattern rel and a join hint") {
    val pattern = PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint = UsingJoinHint(Seq(varFor("a")))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set("a", "b"), hints = Set(hint))
    val context = mockContext()
    val kit = context.plannerState.config.toKit(InterestingOrderConfig.empty, context)

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")
    val bPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "b")

    // when
    val logicalPlans =
      SingleComponentPlanner.planSinglePattern(
        qg,
        kit,
        pattern,
        Map(
          aPlan.availableSymbols.map(_.name) -> BestResults(aPlan, None),
          bPlan.availableSymbols.map(_.name) -> BestResults(bPlan, None)
        ),
        noQPPInnerPlans,
        context
      )

    logicalPlans.toSet should equal(Set(
      `a-r1->b`,
      `b<-r1-a`,
      `a-r1->b = b`,
      `b = a-r1->b`,
      `b<-r1-a = a`,
      `a = b<-r1-a`,
      `expandInto(a X b)`,
      `expandInto(b X a)`
    ))

    assertPlanSolvesHints(
      logicalPlans.filter {
        case join: NodeHashJoin if join.nodes.map(_.name) == Set("a") => true
        case _                                                        => false
      },
      context.staticComponents.planningAttributes.solveds,
      hint
    )
  }

  test("does not plan hashjoins and cartesian product if start and end node are the same") {
    val pattern = PatternRelationship(v"r1", (v"a", v"a"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint = UsingJoinHint(Seq(varFor("a")))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set("a", "a"), hints = Set(hint))
    val context = mockContext()
    val kit = context.plannerState.config.toKit(InterestingOrderConfig.empty, context)

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")

    // when
    val logicalPlans = SingleComponentPlanner.planSinglePattern(
      qg,
      kit,
      pattern,
      Map(aPlan.availableSymbols.map(_.name) -> BestResults(aPlan, None)),
      noQPPInnerPlans,
      context
    )

    logicalPlans.toSet should equal(Set(`a-r1->a`))
  }

  test("plans hashjoins and cartesian product for queries with single pattern rel and a join hint on the end node") {
    val pattern = PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint = UsingJoinHint(Seq(varFor("b")))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set("a", "b"), hints = Set(hint))
    val context = mockContext()
    val kit = context.plannerState.config.toKit(InterestingOrderConfig.empty, context)

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")
    val bPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "b")

    // when
    val logicalPlans =
      SingleComponentPlanner.planSinglePattern(
        qg,
        kit,
        pattern,
        Map(
          aPlan.availableSymbols.map(_.name) -> BestResults(aPlan, None),
          bPlan.availableSymbols.map(_.name) -> BestResults(bPlan, None)
        ),
        noQPPInnerPlans,
        context
      )

    logicalPlans.toSet should equal(Set(
      `a-r1->b`,
      `b<-r1-a`,
      `a-r1->b = b`,
      `b = a-r1->b`,
      `b<-r1-a = a`,
      `a = b<-r1-a`,
      `expandInto(a X b)`,
      `expandInto(b X a)`
    ))

    assertPlanSolvesHints(
      logicalPlans.filter {
        case join: NodeHashJoin if join.nodes.map(_.name) == Set("b") => true
        case _                                                        => false
      },
      context.staticComponents.planningAttributes.solveds,
      hint
    )
  }

  test("plans expands, hashjoins and cartesian product with generic sort only in sensible places") {
    val pattern = PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)
    val hint = UsingJoinHint(Seq(varFor("a")))(pos)
    val qg = QueryGraph(patternRelationships = Set(pattern), patternNodes = Set("a", "b"), hints = Set(hint))
    val context = mockContext()
    val kit = context.plannerState.config.toKit(InterestingOrderConfig.empty, context)

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")
    val bPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "b")

    val aPlanSort = planBuilder()
      .sort("1 ASC")
      .fakeLeafPlan("a")
      .build()
    context.staticComponents.planningAttributes.solveds.copy(aPlan.id, aPlanSort.id)
    context.staticComponents.planningAttributes.providedOrders.copy(aPlan.id, aPlanSort.id)

    val bPlanSort = planBuilder()
      .sort("1 ASC")
      .fakeLeafPlan("b")
      .build()
    context.staticComponents.planningAttributes.solveds.copy(bPlan.id, bPlanSort.id)
    context.staticComponents.planningAttributes.providedOrders.copy(bPlan.id, bPlanSort.id)

    // when
    val logicalPlans =
      SingleComponentPlanner.planSinglePattern(
        qg,
        kit,
        pattern,
        Map(
          aPlan.availableSymbols.map(_.name) -> BestResults(aPlan, Some(aPlanSort)),
          bPlan.availableSymbols.map(_.name) -> BestResults(bPlan, Some(bPlanSort))
        ),
        noQPPInnerPlans,
        context
      )

    logicalPlans.toSet should equal(Set(
      `a-r1->b`,
      `sort(a)-r1->b`,
      `b<-r1-a`,
      `sort(b)<-r1-a`,
      `a-r1->b = b`,
      `a-r1->b = sort(b)`,
      `b = a-r1->b`,
      `b = sort(a)-r1->b`,
      `b<-r1-a = a`,
      `b<-r1-a = sort(a)`,
      `a = b<-r1-a`,
      `a = sort(b)<-r1-a`,
      `expandInto(a X b)`,
      `expandInto(sort(a) X b)`,
      `expandInto(b X a)`,
      `expandInto(sort(b) X a)`
    ))

    assertPlanSolvesHints(
      logicalPlans.filter {
        case join: NodeHashJoin if join.nodes.map(_.name) == Set("a") => true
        case _                                                        => false
      },
      context.staticComponents.planningAttributes.solveds,
      hint
    )
  }

  private def assertPlanSolvesHints(plans: Iterable[LogicalPlan], solveds: Solveds, hints: Hint*): Unit = {
    plans should not be empty
    for (
      h <- hints;
      p <- plans
    ) {
      solveds.get(p.id).asSinglePlannerQuery.lastQueryGraph.hints should contain(h)
    }
  }
}
