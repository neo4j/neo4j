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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
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

  private val patternExp = Exists(PatternExpression(RelationshipsPattern(relChain)_)(Set(varFor("a")), "", ""))_

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
    val result = selectPatternPredicates(aPlan, Set(patternExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SemiApply(aPlan, inner), Set(patternExp))))
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
    val result = selectPatternPredicates(aPlan, Set(notExpr), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(AntiSemiApply(aPlan, inner), Set(notExpr))))
  }

  test("should introduce semi apply for pattern predicate in EXISTS") {
    // Given
    val exists = ExistsSubClause(pattern, None)(InputPosition.NONE, Set(varFor("a")))
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
    val result = selectPatternPredicates(aPlan, Set(exists), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SemiApply(aPlan, inner), Set(exists))))
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
    val result = selectPatternPredicates(aPlan, Set(exists), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SemiApply(aPlan, inner), Set(exists))))
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
    val result = selectPatternPredicates(aPlan, Set(notExists), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(AntiSemiApply(aPlan, inner), Set(notExists))))
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SelectOrSemiApply(aPlan, inner, equalsExp), Set(orsExp))))
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SelectOrSemiApply(aPlan, inner, equalsExp), Set(orsExp))))
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SelectOrAntiSemiApply(aPlan, inner, equalsExp), Set(orsExp))))
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SelectOrAntiSemiApply(aPlan, inner, equalsExp), Set(orsExp))))
  }

  test("should introduce let semi apply and select or semi apply for multiple pattern predicates in or") {
    // Given
    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_)(Set(varFor("a")), "", ""))_

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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(SelectOrSemiApply(LetSemiApply(aPlan, inner, "  UNNAMED0"), inner2, varFor("  UNNAMED0")), Set(orsExp)))
    )
  }

  test("should introduce let semi apply and select or anti semi apply for multiple pattern predicates in or") {
    // Given
    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_)(Set(varFor("a")), "", ""))_
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(SelectOrAntiSemiApply(LetSemiApply(aPlan, inner, "  UNNAMED0"), inner2, varFor("  UNNAMED0")), Set(orsExp)))
    )
  }

  test("should introduce let anti semi apply and select or semi apply for multiple pattern predicates in or") {
    // Given
    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_)(Set(varFor("a")), "", ""))_
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(SelectOrSemiApply(LetAntiSemiApply(aPlan, inner, "  UNNAMED0"), inner2, varFor("  UNNAMED0")), Set(orsExp)))
    )
  }

  test("should introduce let select or semi apply and select or anti semi apply for multiple pattern predicates in or") {
    // Given
    val equalsExp = equals(prop("a", "prop"), literalString("42"))

    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_)(Set(varFor("a")), "", ""))_
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(SelectOrAntiSemiApply(
        LetSelectOrSemiApply(aPlan, inner, "  UNNAMED0", equalsExp), inner2, varFor("  UNNAMED0")
      ), Set(orsExp)))
    )
  }

  test("should introduce let anti select or semi apply and select or semi apply for multiple pattern predicates in or") {
    // Given
    val equalsExp = equals(prop("a", "prop"), literalString("42"))

    val patternExp2 = Exists(PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(varFor("a")), Seq(), None)_,
      RelationshipPattern(Some(varFor("  UNNAMED3")), types, None, None, dir) _,
      NodePattern(Some(varFor("  UNNAMED4")), Seq(), None)_
    )_)_)(Set(varFor("a")), "", ""))_
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(SelectOrSemiApply(
        LetSelectOrAntiSemiApply(aPlan, inner, "  UNNAMED0", equalsExp), inner2, varFor("  UNNAMED0")
      ), Set(orsExp)))
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(SelectOrSemiApply(LetSemiApply(aPlan, inner, "  UNNAMED2"), inner, varFor("  UNNAMED2")), Set(orsExp)))
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
    val result = selectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(SelectOrSemiApply(
        LetAntiSemiApply(aPlan, inner, "  UNNAMED2"), inner, varFor("  UNNAMED2")
      ), Set(orsExp)))
    )
  }
}
