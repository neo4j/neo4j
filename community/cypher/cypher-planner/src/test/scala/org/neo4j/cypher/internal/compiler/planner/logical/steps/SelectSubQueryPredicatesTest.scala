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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SelectSubQueryPredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should not return any candidates when there are no predicates to solve") {
    // Given
    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())
    val inputPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes)

    // When
    val result = SelectSubQueryPredicates(
      input = inputPlan,
      unsolvedPredicates = Set.empty,
      queryGraph = QueryGraph.empty,
      interestingOrderConfig = InterestingOrderConfig.empty,
      context = context
    ).toList

    // Then
    result shouldBe empty
  }

  test("should not solve exists sub-query predicates") {
    // Given
    val subQuery = RegularSinglePlannerQuery(QueryGraph.empty
      .addArgumentId(v"a")
      .addPatternRelationship(PatternRelationship(
        v"r",
        (v"a", v"b"),
        SemanticDirection.OUTGOING,
        Nil,
        SimplePatternLength
      )))

    val existsPredicate = ExistsIRExpression(
      query = subQuery,
      existsVariable = varFor(""),
      solvedExpressionAsString = "exists((a)-[r]->(b))"
    )(
      position = pos,
      computedIntroducedVariables = Some(Set(varFor("r"), varFor("b"))),
      computedScopeDependencies = Some(Set(varFor("a")))
    )

    val qg = QueryGraph(
      patternNodes = Set(v"a"),
      selections = Selections.from(existsPredicate)
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val inputPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")

    // When
    val result = SelectSubQueryPredicates(
      input = inputPlan,
      unsolvedPredicates = Set(existsPredicate),
      queryGraph = qg,
      interestingOrderConfig = InterestingOrderConfig.empty,
      context = context
    ).toList

    // Then
    result shouldBe empty
  }

  test("should not solve predicates containing no sub-query predicates") {
    // Given
    val predicate = propEquality(variable = "n", propKey = "prop", intValue = 42)

    val qg = QueryGraph(
      patternNodes = Set(v"n"),
      selections = Selections.from(predicate)
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val inputPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")

    // When
    val result = SelectSubQueryPredicates(
      input = inputPlan,
      unsolvedPredicates = Set(predicate),
      queryGraph = qg,
      interestingOrderConfig = InterestingOrderConfig.empty,
      context = context
    ).toList

    // Then
    result shouldBe empty
  }

  test("should plan a list sub-query as a rollup apply, ignoring other predicates") {
    // Given
    val labelPredicate = hasLabels("a", "A")
    val singlePredicate = buildSingleIterablePredicate("a", "r", "b")

    val qg = QueryGraph(patternNodes = Set(v"a"), selections = Selections.from(List(labelPredicate, singlePredicate)))

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val inputPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")

    // When
    val result = SelectSubQueryPredicates(
      input = inputPlan,
      unsolvedPredicates = Set(singlePredicate),
      queryGraph = qg,
      interestingOrderConfig = InterestingOrderConfig.empty,
      context = context
    ).toList

    // Then
    val selection = buildSelectionPlan(inputPlan, "a", "r", "b")

    result should equal(List(SelectionCandidate(selection, Set(singlePredicate))))
  }

  test("should plan list sub-queries as individual instances of rollup apply") {
    // Given
    val singlePredicate1 = buildSingleIterablePredicate("a", "r", "b")
    val singlePredicate2 = buildSingleIterablePredicate("a", "s", "c")

    val qg =
      QueryGraph(patternNodes = Set(v"a"), selections = Selections.from(List(singlePredicate1, singlePredicate2)))

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val inputPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, "a")

    // When
    val result = SelectSubQueryPredicates(
      input = inputPlan,
      unsolvedPredicates = Set(singlePredicate1, singlePredicate2),
      queryGraph = qg,
      interestingOrderConfig = InterestingOrderConfig.empty,
      context = context
    ).toList

    // Then
    val selection1 = buildSelectionPlan(inputPlan, "a", "r", "b")
    val selection2 = buildSelectionPlan(inputPlan, "a", "s", "c")

    result should equal(List(
      SelectionCandidate(selection1, Set(singlePredicate1)),
      SelectionCandidate(selection2, Set(singlePredicate2))
    ))
  }

  def buildSingleIterablePredicate(
    argumentName: String,
    relationshipName: String,
    otherNodeName: String
  ): SingleIterablePredicate = {
    val subQueryGraph =
      QueryGraph.empty
        .addArgumentId(varFor(argumentName))
        .addPatternRelationship(PatternRelationship(
          varFor(relationshipName),
          (varFor(argumentName), varFor(otherNodeName)),
          SemanticDirection.OUTGOING,
          Nil,
          SimplePatternLength
        ))

    val subQuery = RegularSinglePlannerQuery(
      queryGraph = subQueryGraph,
      horizon = RegularQueryProjection(Map(v"item" -> literalInt(1)))
    )

    val listIRExpression = ListIRExpression(
      query = subQuery,
      variableToCollect = varFor("item"),
      collection = varFor("items"),
      solvedExpressionAsString = s"[($argumentName)-[$relationshipName]->($otherNodeName) | 1]"
    )(
      position = pos,
      computedIntroducedVariables = Some(Set(varFor(relationshipName), varFor(otherNodeName))),
      computedScopeDependencies = Some(Set(varFor(argumentName)))
    )

    SingleIterablePredicate(
      scope = FilterScope(varFor("x"), Some(trueLiteral))(pos),
      expression = listIRExpression
    )(pos)
  }

  def buildSelectionPlan(
    inputPlan: LogicalPlan,
    argumentName: String,
    relationshipName: String,
    otherNodeName: String
  ): Selection = {
    val argument = Argument(Set(varFor(argumentName)))

    val expand = Expand(
      source = argument,
      from = varFor(argumentName),
      dir = SemanticDirection.OUTGOING,
      types = Nil,
      to = varFor(otherNodeName),
      relName = varFor(relationshipName),
      mode = ExpandAll
    )

    val projection = Projection(
      source = expand,
      projectExpressions = Map(varFor("item") -> literalInt(1))
    )

    val rollUpApply = RollUpApply(
      left = inputPlan,
      right = projection,
      collectionName = varFor("items"),
      variableToCollect = varFor("item")
    )

    val rewrittenSinglePredicate = SingleIterablePredicate(
      scope = FilterScope(varFor("x"), Some(trueLiteral))(pos),
      expression = varFor("items")
    )(pos)

    Selection(
      predicates = List(rewrittenSinglePredicate),
      source = rollUpApply
    )
  }
}
