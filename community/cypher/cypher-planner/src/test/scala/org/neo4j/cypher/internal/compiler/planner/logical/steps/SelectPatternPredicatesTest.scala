/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SelectPatternPredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val dir = SemanticDirection.OUTGOING
  private val types = Seq.empty[RelTypeName]
  private val nodeName = "  UNNAMED2"
  private val patternRel = PatternRelationship("  UNNAMED1", ("a", nodeName), dir, types, SimplePatternLength)

  // MATCH (a) WHERE (a)-->()
  private val relChain = RelationshipChain(
    NodePattern(Some(varFor("a")), Seq(), None)_,
    RelationshipPattern(Some(varFor("  UNNAMED1")), types, None, None, dir) _,
    NodePattern(Some(varFor(nodeName)), Seq(), None)_
  )_

  private val patternExp = Exists(PatternExpression(RelationshipsPattern(relChain)_))_

  private val pattern: Pattern = Pattern(Seq(EveryPath(relChain)))_

  test("should introduce semi apply for unsolved exclusive pattern predicate") {
    // Given
    val predicate = Predicate(Set("a"), patternExp)
    val selections = Selections(Set(predicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(SemiApply(aPlan, inner)))
  }

  test("should introduce anti semi apply for unsolved exclusive negated pattern predicate") {
    val notExpr = not(patternExp)
    // Given
    val predicate = Predicate(Set("a"), notExpr)
    val selections = Selections(Set(predicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(AntiSemiApply(aPlan, inner)))
  }

  test("should introduce semi apply for pattern predicate in EXISTS") {
    // Given
    val exists = ExistsSubClause(pattern, None)(InputPosition.NONE, Set.empty)
    val predicate = Predicate(Set("a"), exists)
    val selections = Selections(Set(predicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(SemiApply(aPlan, inner)))
  }

  test("should introduce semi apply for pattern predicate in EXISTS where node variable comes from outer scope") {
    // Given
    val exists = ExistsSubClause(pattern, None)(InputPosition.NONE, Set(Variable("a")_))
    val predicate = Predicate(Set("a"), exists)
    val selections = Selections(Set(predicate))

    val qg = QueryGraph(
      patternNodes = Set.empty,
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(SemiApply(aPlan, inner)))
  }

  test("should introduce anti semi apply for negated pattern predicate in EXISTS") {
    // Given
    val notExists = not(ExistsSubClause(pattern, None)(InputPosition.NONE, Set(Variable("a")_)))
    val predicate = Predicate(Set("a"), notExists)
    val selections = Selections(Set(predicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(AntiSemiApply(aPlan, inner)))
  }

  test("should not introduce semi apply for unsolved exclusive pattern predicate when nodes not applicable") {
    // Given
    val predicate = Predicate(Set("a"), patternExp)
    val selections = Selections(Set(predicate))

    val qg = QueryGraph(
      patternNodes = Set("b"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val bPlan = newMockedLogicalPlan(context.planningAttributes, "b")
    // When
    val result = selectPatternPredicates(bPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq.empty)
  }

  test("should introduce select or semi apply for unsolved pattern predicates in disjunction with expressions") {
    // Given
    val equalsExp = equals(prop("a", "prop"), literalString("42"))
    val orsExp = ors(patternExp, equalsExp)
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val argument = Argument(Set("a"))
    val inner = Expand(argument, "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(SelectOrSemiApply(aPlan, inner, equalsExp)))
  }

    test("should introduce select or semi apply for unsolved exists predicates in disjunction with expressions") {
    // Given
    val equalsExp = equals(prop("a", "prop"), literalString("42"))
    val exists = ExistsSubClause(pattern, None)(InputPosition.NONE, Set(Variable("a")_))
    val orsExp = ors(exists, equalsExp)
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val argument = Argument(Set("a"))
    val inner = Expand(argument, "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(SelectOrSemiApply(aPlan, inner, equalsExp)))
  }

  test("should introduce select or anti semi apply for unsolved negated pattern predicates in disjunction with an expression") {
    // Given
    val equalsExp = equals(prop("a", "prop"), literalString("42"))
    val orsExp = ors(not(patternExp), equalsExp)
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(SelectOrAntiSemiApply(aPlan, inner, equalsExp)))
  }

  test("should introduce select or anti semi apply for unsolved negated exists predicates in disjunction with an expression") {
    // Given
    val equalsExp = equals(prop("a", "prop"), literalString("42"))
    val exists = ExistsSubClause(pattern, None)(InputPosition.NONE, Set(Variable("a")_))
    val orsExp = ors(not(exists), equalsExp)
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(Seq(SelectOrAntiSemiApply(aPlan, inner, equalsExp)))
  }

  test("should introduce let semi apply and select or semi apply for multiple pattern predicates in or") {
    // Given
    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_))_

    val patternRel2 = PatternRelationship("  UNNAMED3", ("a", "  UNNAMED4"), dir, types, SimplePatternLength)

    val orsExp = ors(patternExp, patternExp2)
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)
    val inner2 = Expand(Argument(Set("a")), "a", dir, types, "  UNNAMED4", patternRel2.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(
      Seq(SelectOrSemiApply(LetSemiApply(aPlan, inner, "  FRESHID0"), inner2, varFor("  FRESHID0")))
    )
  }

  test("should introduce let semi apply and select or anti semi apply for multiple pattern predicates in or") {
    // Given
    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_))_
    val patternRel2 = PatternRelationship("  UNNAMED3", ("a", "  UNNAMED4"), dir, types, SimplePatternLength)

    val orsExp = ors(patternExp, not(patternExp2))
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)
    val inner2 = Expand(Argument(Set("a")), "a", dir, types, "  UNNAMED4", patternRel2.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(
      Seq(SelectOrAntiSemiApply(LetSemiApply(aPlan, inner, "  FRESHID0"), inner2, varFor("  FRESHID0")))
    )
  }

  test("should introduce let anti semi apply and select or semi apply for multiple pattern predicates in or") {
    // Given
    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_))_
    val patternRel2 = PatternRelationship("  UNNAMED3", ("a", "  UNNAMED4"), dir, types, SimplePatternLength)

    val orsExp = ors(not(patternExp), patternExp2)
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)
    val inner2 = Expand(Argument(Set("a")), "a", dir, types, "  UNNAMED4", patternRel2.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(
      Seq(SelectOrSemiApply(LetAntiSemiApply(aPlan, inner, "  FRESHID0"), inner2, varFor("  FRESHID0")))
    )
  }

  test("should introduce let select or semi apply and select or anti semi apply for multiple pattern predicates in or") {
    // Given
    val equalsExp = equals(prop("a", "prop"), literalString("42"))

    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_))_
    val patternRel2 = PatternRelationship("  UNNAMED3", ("a", "  UNNAMED4"), dir, types, SimplePatternLength)

    val orsExp = ors(equalsExp, patternExp, not(patternExp2))
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)
    val inner2 = Expand(Argument(Set("a")), "a", dir, types, "  UNNAMED4", patternRel2.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(
      Seq(SelectOrAntiSemiApply(
        LetSelectOrSemiApply(aPlan, inner, "  FRESHID0", equalsExp), inner2, varFor("  FRESHID0")
      ))
    )
  }

  test("should introduce let anti select or semi apply and select or semi apply for multiple pattern predicates in or") {
    // Given
    val equalsExp = equals(prop("a", "prop"), literalString("42"))

    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_))_
    val patternRel2 = PatternRelationship("  UNNAMED3", ("a", "  UNNAMED4"), dir, types, SimplePatternLength)

    val orsExp = ors(equalsExp, not(patternExp), patternExp2)
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)
    val inner2 = Expand(Argument(Set("a")), "a", dir, types, "  UNNAMED4", patternRel2.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(
      Seq(SelectOrSemiApply(
        LetSelectOrAntiSemiApply(aPlan, inner, "  FRESHID0", equalsExp), inner2, varFor("  FRESHID0")
      ))
    )
  }

  test("should introduce select or semi apply for exists predicates and other pattern predicate in or") {
    // Given
    val exists = ExistsSubClause(pattern, None)(InputPosition.NONE, Set(Variable("a")_))
    val orsExp = ors(exists, patternExp)
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(
      Seq(SelectOrSemiApply(LetSemiApply(aPlan, inner, "  FRESHID0"), inner, varFor("  FRESHID0")))
    )
  }

  test("should introduce anti select or semi apply for exists predicates and other pattern predicate in or") {
    // Given
    val exists = ExistsSubClause(pattern, None)(InputPosition.NONE, Set(Variable("a")_))
    val orsExp = ors(not(exists), patternExp)
    val orPredicate = Predicate(Set("a"), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set("a"),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.planningAttributes, "a")
    val inner = Expand(Argument(Set("a")), "a", dir, types, nodeName, patternRel.name, ExpandAll)

    // When
    val result = selectPatternPredicates(aPlan, qg, InterestingOrder.empty, context)

    // Then
    result should equal(
      Seq(SelectOrSemiApply(
        LetAntiSemiApply(aPlan, inner, "  FRESHID0"), inner, varFor("  FRESHID0")
      ))
    )
  }
}
