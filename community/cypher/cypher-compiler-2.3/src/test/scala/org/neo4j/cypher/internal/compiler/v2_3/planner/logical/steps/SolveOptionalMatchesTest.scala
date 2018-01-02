/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, LogicalPlanningContext, QueryPlannerConfiguration}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, LogicalPlanningTestSupport, PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, SemanticTable, ast}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SolveOptionalMatchesTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val defaultSolvers = QueryPlannerConfiguration.default.optionalSolvers
  private val pickBest = QueryPlannerConfiguration.default.pickBestCandidate

  val patternRelAtoB = newPatternRelationship("a", "b", "r1")
  val patternRelAtoC = newPatternRelationship("a", "c", "r2")
  val patternRelCtoX = newPatternRelationship("c", "x", "r3")

  // GIVEN (a) OPTIONAL MATCH (a)-[r1]->(b)
  val qgForAtoB = QueryGraph(
    patternNodes = Set("a", "b"),
    patternRelationships = Set(patternRelAtoB)
  )

  // GIVEN (a) OPTIONAL MATCH (a)-[r2]->(c)
  val qgForAtoC = QueryGraph(
    patternNodes = Set("a", "c"),
    patternRelationships = Set(patternRelAtoC)
  )

  // GIVEN (c) OPTIONAL MATCH (c)-[r3]->(x)
  val qgForCtoX = QueryGraph(
    patternNodes = Set("c", "x"),
    patternRelationships = Set(patternRelCtoX)
  )

  test("should introduce apply for unsolved optional match when all arguments are covered") {
    // Given MATCH a OPTIONAL MATCH (a)-[r]->(b)
    val qg = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(qgForAtoB)
    val lhs = newMockedLogicalPlan("a")

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any())).thenReturn((plan: PlannerQuery, _: QueryGraphSolverInput, _: SemanticTable) => plan match {
      case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set(IdName("a")) &&
                                             queryGraph.patternRelationships.isEmpty => Cardinality(1.0)
      case _            => Cardinality(1000.0)
    })

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(hardcodedStatistics)
    )
    val planTable = greedyPlanTableWith(lhs)

    // When
    val resultingPlanTable = solveOptionalMatches(defaultSolvers, pickBest(context))(planTable, qg)

    // Then
    val expectedRhs = Expand(Argument(Set("a"))(solved)(), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1")(solved)
    val expectedResult = Apply(lhs, Optional(expectedRhs)(solved))(solved)

    resultingPlanTable.plans should have size 1
    resultingPlanTable.plans.head should equal(expectedResult)
  }

  test("should not try to solve optional match if cartesian product still needed") {
    // Given
    val qg = QueryGraph(
      patternNodes = Set("a", "b") // MATCH a, b
    ).withAddedOptionalMatch(qgForAtoB.withArgumentIds(Set("a", "b"))) // GIVEN a, b OPTIONAL MATCH a-[r]->b

    implicit val context = createLogicalPlanContext()
    val planForA = newMockedLogicalPlan("a")
    val planForB = newMockedLogicalPlan("b")
    val planTable = greedyPlanTableWith(planForA, planForB)

    // When
    val resultingPlanTable = solveOptionalMatches(defaultSolvers, pickBest(context))(planTable, qg)

    // Then
    resultingPlanTable should equal(planTable)
  }

  test("should not try to solve optional match if already solved by input plan") {
    // Given
    val qg = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(qgForAtoB)

    implicit val context = createLogicalPlanContext()
    val inputPlan = newMockedLogicalPlanWithSolved(
      Set("a", "b"),
      solved.withGraph(qg))

    val planTable = greedyPlanTableWith(inputPlan)

    // When
    val resultingPlanTable = solveOptionalMatches(defaultSolvers, pickBest(context))(planTable, qg)

    // Then
    resultingPlanTable should equal(planTable)
  }

  test("when given two optional matches, and one is already solved, will solve the next") {
    // Given MATCH a OPTIONAL MATCH a-->b OPTIONAL MATCH a-->c
    val qgWithFirstOptionalMatch = QueryGraph(patternNodes = Set("a")).
                                   withAddedOptionalMatch(qgForAtoB)
    val qg = qgWithFirstOptionalMatch.
             withAddedOptionalMatch(qgForAtoC)

    implicit val context = createLogicalPlanContext()
    val inputPlan = newMockedLogicalPlanWithSolved(
      Set("a", "b"),
      solved.withGraph(qgWithFirstOptionalMatch))

    val planTable = greedyPlanTableWith(inputPlan)


    // when
    // Using a simpler strategy to avoid costs
    val resultingPlanTable = (new solveOptionalMatches(Seq(applyOptional), pickBest(context)))(planTable, qg)

    // Then
    val innerPlan = Expand(Argument(Set("a"))(solved)(), "a", SemanticDirection.OUTGOING, Seq.empty, "c", "r2")(solved)
    val applyPlan = Apply(inputPlan, Optional(innerPlan)(solved))(solved)

    resultingPlanTable.plans should have size 1
    resultingPlanTable.plans.head should equal(applyPlan)
  }

  test("test OPTIONAL MATCH a RETURN a") {
    // OPTIONAL MATCH a
    val qg = QueryGraph.empty.withOptionalMatches(Seq(QueryGraph(patternNodes = Set(IdName("a")))))
    implicit val context = createLogicalPlanContext()
    val planTable = greedyPlanTableWith()

    // When
    val resultingPlanTable = solveOptionalMatches(defaultSolvers, pickBest(context))(planTable, qg)

    // Then
    val singleRow: LogicalPlan = SingleRow()(solved)
    val expectedRhs: LogicalPlan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val applyPlan = Apply(singleRow, Optional(expectedRhs)(solved))(solved)

    resultingPlanTable.plans should have size 1
    resultingPlanTable.plans.head should equal(applyPlan)
  }

  test("two optional matches to solve - the first one is solved and the second is N/A") {
    // Given MATCH a, c OPTIONAL MATCH a-->b OPTIONAL MATCH a-->c
    val qgWithFirstOptionalMatch = QueryGraph(patternNodes = Set("a", "c")).
                                   withAddedOptionalMatch(qgForAtoB)
    val qg = qgWithFirstOptionalMatch.
             withAddedOptionalMatch(qgForAtoC)

    implicit val context = createLogicalPlanContext()
    val lhs = newMockedLogicalPlanWithSolved(
      Set("a", "b", "r1"),
      solved.withGraph(qgWithFirstOptionalMatch))

    val planForC = newMockedLogicalPlan("c")

    val planTable = greedyPlanTableWith(lhs,planForC)

    // When
    val resultingPlanTable = solveOptionalMatches(defaultSolvers, pickBest(context))(planTable, qg)

    // Then
    resultingPlanTable should equal(planTable)
  }

  test("should handle multiple optional matches building off of different plans in the plan table") {
    // Given MATCH a, c OPTIONAL MATCH a-->b OPTIONAL MATCH a-->c
    val qgWithFirstOptionalMatch = QueryGraph(patternNodes = Set("a", "c")).
                                   withAddedOptionalMatch(qgForAtoB)
    val qg = qgWithFirstOptionalMatch.
             withAddedOptionalMatch(qgForAtoC)

    implicit val context = createLogicalPlanContext()
    val lhs = newMockedLogicalPlanWithSolved(
      Set("a", "b", "r1"),
      solved.withGraph(qgWithFirstOptionalMatch))

    val planForC = newMockedLogicalPlan("c")

    val planTable = greedyPlanTableWith(lhs,planForC)


    // When
    val resultingPlanTable = solveOptionalMatches(defaultSolvers, pickBest(context))(planTable, qg)

    // Then
    resultingPlanTable should equal(planTable)
  }

  test("given plan table with two plans, and the ability to solve one optional match based on a plan, do it") {
    // MATCH a, c OPTIONAL MATCH a-->b
    val qg = QueryGraph(patternNodes = Set("a", "c")).withAddedOptionalMatch(qgForAtoB)
    implicit val context = createLogicalPlanContext()
    val planForA = newMockedLogicalPlan("a")
    val planForC = newMockedLogicalPlan("c")

    val planTable = greedyPlanTableWith(planForC, planForA)

    // when
    // Using a simpler strategy to avoid costs
    val resultingPlanTable = solveOptionalMatches(Seq(applyOptional), pickBest(context))(planTable, qg)
    val expectedRhs = Expand(Argument(Set("a"))(solved)(), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1")(solved)
    val expectedResult = Apply(planForA, Optional(expectedRhs)(solved))(solved)

    resultingPlanTable.plans should have size 2
    val qgOptionalMatch = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(qgForAtoB)
    resultingPlanTable(qgOptionalMatch) should equal(expectedResult)
  }

  test("given plan table with multiple plans, and multiple optional matches that can be built on top of them, solve them all") {
    // MATCH a, c OPTIONAL MATCH a-->b OPTIONAL MATCH c-->x
    val qg = QueryGraph(patternNodes = Set("a", "c")).
    withAddedOptionalMatch(qgForAtoB).
    withAddedOptionalMatch(qgForCtoX)

    implicit val context = createLogicalPlanContext()
    val planForA = newMockedLogicalPlan("a")
    val planForC = newMockedLogicalPlan("c")

    val planTable = greedyPlanTableWith(planForC, planForA)

    // when
    // Using a simpler strategy to avoid costs
    val step1 = solveOptionalMatches(Seq(applyOptional), pickBest(context))(planTable, qg)
    val resultingPlanTable = solveOptionalMatches(Seq(applyOptional), pickBest(context))(step1, qg)

    val expectedPlanForAtoB = Expand(Argument(Set("a"))(solved)(), "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1")(solved)
    val expectedResult1 = Apply(planForA, Optional(expectedPlanForAtoB)(solved))(solved)

    val expectedPlanForCtoX = Expand(Argument(Set("c"))(solved)(), "c", SemanticDirection.OUTGOING, Seq.empty, "x", "r3")(solved)
    val expectedResult2 = Apply(planForC, Optional(expectedPlanForCtoX)(solved))(solved)

    resultingPlanTable.plans should have size 2
    val qgOptionalMatchAB = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(qgForAtoB)
    resultingPlanTable(qgOptionalMatchAB) should equal(expectedResult1)
    val qgOptionalMatchCX = QueryGraph(patternNodes = Set("c")).withAddedOptionalMatch(qgForCtoX)
    resultingPlanTable(qgOptionalMatchCX) should equal(expectedResult2)
  }

  test("should introduce apply for unsolved optional match and solve predicates on the pattern") {
    // Given MATCH a OPTIONAL MATCH (a)-[r]->(b:X)
    val labelPredicate: ast.Expression = ast.HasLabels(ident("b"), Seq(ast.LabelName("X")_ )) _
    val qg = QueryGraph(patternNodes = Set("a")).
             withAddedOptionalMatch(qgForAtoB.addPredicates(labelPredicate))

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any())).thenReturn((plan: PlannerQuery, _: QueryGraphSolverInput, _: SemanticTable) => plan match {
      case PlannerQuery(queryGraph, _, _) if queryGraph.argumentIds == Set(IdName("a")) &&
                                             queryGraph.patternNodes == Set(IdName("a")) &&
                                             queryGraph.patternRelationships.isEmpty  => Cardinality(1.0)
      case _            => Cardinality(1000.0)
    })

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(hardcodedStatistics)
    )
    val lhs = newMockedLogicalPlan("a")
    val planTable = greedyPlanTableWith(lhs)

    // when
    // Using a simpler strategy to avoid costs
    val resultingPlanTable = solveOptionalMatches(Seq(applyOptional), pickBest(context))(planTable, qg)

    // Then

    val arguments = Argument(Set("a"))(solved)()
    val expand = Expand(
      arguments,
      "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1")(solved)
    val expectedRhs = Selection(Seq(labelPredicate), expand)(solved)

    val expectedResult = Apply(lhs, Optional(expectedRhs)(solved))(solved)

    resultingPlanTable.plans should have size 1
    resultingPlanTable.plans.head should equal(expectedResult)
  }

  private def createLogicalPlanContext(): LogicalPlanningContext = {
    val factory = newMockedMetricsFactory
    newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(hardcodedStatistics)
    )
  }

  private implicit def lift(plannerQuery: PlannerQuery): PlannerQuery with CardinalityEstimation =
    CardinalityEstimation.lift(plannerQuery, Cardinality(0))
}
